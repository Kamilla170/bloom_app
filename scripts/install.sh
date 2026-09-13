#!/usr/bin/env bash
# Разовая установка: кладёт симлинки в /usr/local/bin, чтобы команды
# работали из любой папки. Запускать от root:  bash scripts/install.sh
set -euo pipefail
REPO="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)"
BIN=/usr/local/bin
for c in bloom-pro bloom-pro-revoke bloom-users; do
  chmod +x "$REPO/scripts/$c"
  ln -sf "$REPO/scripts/$c" "$BIN/$c"
  echo "  $BIN/$c -> $REPO/scripts/$c"
done
echo
echo "Готово. Проверь:  bloom-users"
