"""
Админские операции с подписками из командной строки.

Запускается ВНУТРИ контейнера api (нужен доступ к БД и services/), но сам
файл живёт на хосте — обёртки в scripts/ передают его в контейнер через
stdin. Поэтому после `git pull` пересобирать образ не нужно: выполняется
всегда та версия, что лежит в репозитории.

Команды:
    grant  EMAIL [EMAIL...] [--days N]   выдать PRO
    revoke EMAIL [EMAIL...]              снять PRO
    users  [--days N] [--all]            кто регистрировался

Про поиск по email. Вход через Яндекс сохраняет тот адрес, который вернул
Яндекс ID, — он часто не совпадает с тем, что человек назвал. Поэтому при
точном промахе ищем по подстроке в email и имени и показываем кандидатов,
а не молча отвечаем «не найден».
"""

import argparse
import asyncio
import sys
from datetime import date

from database import get_db
from services.subscription_service import activate_pro, revoke_pro, get_user_plan
from config import ADMIN_USER_IDS

OK, MISS, WARN = "[OK]", "[--]", "[!!]"

# Косметическая метка в subscription_events: при granted_by событие в любом
# случае классифицируется как granted_by_admin (см. _classify_subscription_event).
GRANT_PLAN_ID = "1month"


async def _find_exact(conn, email):
    return await conn.fetch(
        "SELECT user_id, email, auth_provider, first_name "
        "FROM users WHERE lower(email) = lower($1) ORDER BY user_id",
        email,
    )


async def _find_similar(conn, needle):
    """Кандидаты по подстроке — на случай, если Яндекс вернул другой адрес."""
    local = needle.split("@")[0]
    return await conn.fetch(
        "SELECT user_id, email, auth_provider, first_name, created_at::date AS reg "
        "FROM users "
        "WHERE email ILIKE '%' || $1 || '%' OR first_name ILIKE '%' || $1 || '%' "
        "ORDER BY created_at DESC LIMIT 10",
        local,
    )


def _suggest(rows, email):
    print(f"{MISS} {email}: точного совпадения нет.")
    if not rows:
        print("     Похожих тоже нет — человек ещё ни разу не входил в приложение.")
        print("     Пусть войдёт, потом повтори команду.")
        return
    print("     Возможно, это кто-то из них (вход через Яндекс мог дать другой адрес):")
    for r in rows:
        name = r["first_name"] or "—"
        print(f"       {r['email']:35} {r['auth_provider']:8} {name:12} рег. {r['reg']}")
    print("     Запусти команду ещё раз с нужным адресом из списка.")


async def cmd_grant(args):
    db = await get_db()
    failed = False
    for email in args.emails:
        async with db.pool.acquire() as conn:
            rows = await _find_exact(conn, email)
            if not rows:
                _suggest(await _find_similar(conn, email), email)
                failed = True
                continue
        for r in rows:
            before = await get_user_plan(r["user_id"])
            exp = await activate_pro(
                r["user_id"],
                days=args.days,
                amount=0,
                granted_by=ADMIN_USER_IDS[0],
                plan_id=GRANT_PLAN_ID,
                source="admin",
            )
            was = "" if before["plan"] == "free" else f" (было до {before['expires_at']:%d.%m.%Y}, продлили)"
            via = f"вход через {r['auth_provider']}"
            print(f"{OK} {r['email']} ({via}): PRO до {exp:%d.%m.%Y}{was}")
    return 1 if failed else 0


async def cmd_revoke(args):
    db = await get_db()
    failed = False
    for email in args.emails:
        async with db.pool.acquire() as conn:
            rows = await _find_exact(conn, email)
            if not rows:
                _suggest(await _find_similar(conn, email), email)
                failed = True
                continue
        for r in rows:
            plan = await get_user_plan(r["user_id"])
            if plan["plan"] == "free":
                print(f"{WARN} {r['email']}: и так на free, ничего не меняю")
                continue
            await revoke_pro(r["user_id"])
            print(f"{OK} {r['email']}: PRO снят (был до {plan['expires_at']:%d.%m.%Y})")
    return 1 if failed else 0


async def cmd_users(args):
    db = await get_db()
    where = "" if args.all else "WHERE u.created_at > now() - make_interval(days => $1)"
    params = [] if args.all else [args.days]
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT u.user_id, u.email, u.auth_provider, u.first_name, "
            "       u.created_at::date AS reg, "
            "       coalesce(s.plan, 'free') AS plan, s.expires_at "
            "FROM users u LEFT JOIN subscriptions s ON s.user_id = u.user_id "
            f"{where} ORDER BY u.created_at DESC",
            *params,
        )
    if not rows:
        print("Пусто.")
        return 0
    print(f"{'email':35} {'вход':8} {'имя':12} {'рег.':11} подписка")
    print("-" * 88)
    for r in rows:
        # В таблице plan может остаться 'pro' у истёкшей подписки: она
        # переводится на free лениво, при следующем входе пользователя.
        sub = "free"
        if r["plan"] == "pro" and r["expires_at"]:
            live = r["expires_at"].date() >= date.today()
            sub = f"PRO до {r['expires_at']:%d.%m.%Y}" + ("" if live else " (истекла)")
        name = r["first_name"] or "—"
        print(f"{r['email'] or '—':35} {r['auth_provider']:8} {name:12} {r['reg']}  {sub}")
    print(f"\nвсего: {len(rows)}")
    return 0


def main():
    p = argparse.ArgumentParser(prog="bloom-admin", description="Подписки Bloom AI")
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("grant", help="выдать PRO")
    g.add_argument("emails", nargs="+")
    g.add_argument("--days", type=int, default=14,
                   help="на сколько дней (по умолчанию 14 — под окно теста Google Play)")
    g.set_defaults(fn=cmd_grant)

    r = sub.add_parser("revoke", help="снять PRO")
    r.add_argument("emails", nargs="+")
    r.set_defaults(fn=cmd_revoke)

    u = sub.add_parser("users", help="кто регистрировался")
    u.add_argument("--days", type=int, default=7)
    u.add_argument("--all", action="store_true", help="вообще все")
    u.set_defaults(fn=cmd_users)

    # Короткая форма без флага: `bloom-pro почта 20` == `... --days 20`.
    argv = sys.argv[1:]
    if (len(argv) > 2 and argv[0] == "grant"
            and argv[-1].isdigit() and argv[-2] != "--days"):
        argv = argv[:-1] + ["--days", argv[-1]]

    args = p.parse_args(argv)
    sys.exit(asyncio.run(args.fn(args)))


if __name__ == "__main__":
    main()
