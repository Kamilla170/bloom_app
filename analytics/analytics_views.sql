-- ============================================================
-- Bloom AI — аналитические view (схема analytics)
-- Перенос с railway-базы на Yandex.
-- Порядок учитывает зависимости между view (снизу вверх по слоям).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- ============================================================
-- СЛОЙ 0: базовые справочники и активность (ни от кого не зависят)
-- ============================================================

CREATE OR REPLACE VIEW analytics._plan_days AS
 SELECT plan_id, days
   FROM ( VALUES ('1month'::text,30), ('3months'::text,90), ('6months'::text,180), ('12months'::text,365)) t(plan_id, days);

CREATE OR REPLACE VIEW analytics._user_active_days AS
 WITH all_activity AS (
         SELECT users.user_id, date(users.last_activity) AS day
           FROM users WHERE (users.last_activity IS NOT NULL)
        UNION
         SELECT care_history.user_id, date(care_history.action_date) AS date
           FROM care_history WHERE ((care_history.user_id IS NOT NULL) AND (care_history.action_date IS NOT NULL))
        UNION
         SELECT plant_qa_history.user_id, date(plant_qa_history.question_date) AS date
           FROM plant_qa_history WHERE ((plant_qa_history.user_id IS NOT NULL) AND (plant_qa_history.question_date IS NOT NULL))
        UNION
         SELECT plant_analyses_full.user_id, date(plant_analyses_full.analysis_date) AS date
           FROM plant_analyses_full WHERE ((plant_analyses_full.user_id IS NOT NULL) AND (plant_analyses_full.analysis_date IS NOT NULL))
        )
 SELECT DISTINCT user_id, day FROM all_activity;

CREATE OR REPLACE VIEW analytics._user_active_months AS
 WITH all_activity AS (
         SELECT users.user_id, (date_trunc('month'::text, users.last_activity))::date AS month
           FROM users WHERE (users.last_activity IS NOT NULL)
        UNION
         SELECT care_history.user_id, (date_trunc('month'::text, care_history.action_date))::date AS date_trunc
           FROM care_history WHERE ((care_history.user_id IS NOT NULL) AND (care_history.action_date IS NOT NULL))
        UNION
         SELECT plant_qa_history.user_id, (date_trunc('month'::text, plant_qa_history.question_date))::date AS date_trunc
           FROM plant_qa_history WHERE ((plant_qa_history.user_id IS NOT NULL) AND (plant_qa_history.question_date IS NOT NULL))
        UNION
         SELECT plant_analyses_full.user_id, (date_trunc('month'::text, plant_analyses_full.analysis_date))::date AS date_trunc
           FROM plant_analyses_full WHERE ((plant_analyses_full.user_id IS NOT NULL) AND (plant_analyses_full.analysis_date IS NOT NULL))
        )
 SELECT DISTINCT user_id, month FROM all_activity;

CREATE OR REPLACE VIEW analytics.v_plans AS
 SELECT plan_id, label, days, regular_price, discount_price,
    round((((regular_price)::numeric * (30)::numeric) / (days)::numeric), 2) AS regular_mrr,
    round((((discount_price)::numeric * (30)::numeric) / (days)::numeric), 2) AS discount_mrr
   FROM ( VALUES ('1month'::text,'1 месяц'::text,30,249,169), ('3months'::text,'3 месяца'::text,90,599,399), ('6months'::text,'6 месяцев'::text,180,1099,739), ('12months'::text,'12 месяцев'::text,365,2099,1369)) p(plan_id, label, days, regular_price, discount_price)
 LIMIT 100;

-- ============================================================
-- СЛОЙ 1: зависят только от таблиц или от v_plans
-- ============================================================

CREATE OR REPLACE VIEW analytics.v_active_subscriptions AS
 SELECT s.user_id, s.plan_id, s.plan_amount, s.plan_days, s.expires_at,
    (s.auto_pay_method_id IS NOT NULL) AS has_auto_pay,
    round((((s.plan_amount)::numeric * (30)::numeric) / (NULLIF(s.plan_days, 0))::numeric), 2) AS mrr_rub,
        CASE
            WHEN (s.expires_at > now()) THEN 'active'::text
            WHEN ((s.expires_at + '3 days'::interval) > now()) THEN 'grace'::text
            ELSE 'expired'::text
        END AS status,
    s.created_at AS sub_created_at, s.updated_at AS sub_updated_at,
    u.created_at AS user_created_at, u.utm_source
   FROM (subscriptions s JOIN users u ON ((u.user_id = s.user_id)))
  WHERE ((s.plan = 'pro'::text) AND (s.expires_at IS NOT NULL) AND ((s.expires_at + '3 days'::interval) > now()) AND (s.granted_by_admin IS NULL))
 LIMIT 100;

CREATE OR REPLACE VIEW analytics.v_churned_users AS
 WITH user_pro_history AS (
         SELECT payments.user_id, min(payments.created_at) AS first_pro_at, max(payments.created_at) AS last_payment_at
           FROM payments WHERE (payments.status = 'succeeded'::text) GROUP BY payments.user_id
        ), last_event AS (
         SELECT DISTINCT ON (subscription_events.user_id) subscription_events.user_id,
            subscription_events.event_type AS last_event_type, subscription_events.created_at AS last_event_at
           FROM subscription_events
          WHERE (subscription_events.event_type = ANY (ARRAY['payment_failed'::text, 'auto_pay_disabled'::text, 'cancelled'::text, 'revoked_by_admin'::text]))
          ORDER BY subscription_events.user_id, subscription_events.created_at DESC
        )
 SELECT s.user_id, uph.first_pro_at, uph.last_payment_at, s.expires_at AS churned_at, s.plan_id AS last_plan_id,
    GREATEST((EXTRACT(day FROM (s.expires_at - uph.first_pro_at)))::integer, 0) AS days_to_churn,
        CASE
            WHEN ((le.last_event_type = 'payment_failed'::text) AND (le.last_event_at >= (s.expires_at - '14 days'::interval))) THEN 'involuntary'::text
            WHEN (le.last_event_type = 'revoked_by_admin'::text) THEN 'admin_revoked'::text
            ELSE 'voluntary'::text
        END AS churn_type
   FROM ((subscriptions s
     JOIN user_pro_history uph ON ((uph.user_id = s.user_id)))
     LEFT JOIN last_event le ON ((le.user_id = s.user_id)))
  WHERE ((s.expires_at IS NOT NULL) AND (s.expires_at < (now() - '7 days'::interval)));

CREATE OR REPLACE VIEW analytics.v_payments_clean AS
 SELECT p.id, p.payment_id, p.user_id, p.amount AS amount_rub, p.currency, p.status,
    p.is_recurring, p.plan_id, p.created_at, p.updated_at,
    pl.days AS plan_days, pl.regular_price, pl.discount_price,
    (row_number() OVER (PARTITION BY p.user_id ORDER BY p.created_at) = 1) AS is_first_payment
   FROM (payments p LEFT JOIN analytics.v_plans pl ON ((pl.plan_id = (p.plan_id)::text)))
  WHERE (p.status = 'succeeded'::text)
 LIMIT 100;

CREATE OR REPLACE VIEW analytics.v_ai_cost_by_feature AS
 SELECT request_type, count(*) AS requests, count(DISTINCT user_id) AS unique_users,
    round(sum(cost_usd), 4) AS total_cost_usd,
    round((sum(cost_usd) * (95)::numeric), 2) AS total_cost_rub,
    round((avg(cost_usd) * (95)::numeric), 2) AS avg_cost_per_request_rub,
    round(avg(latency_ms), 0) AS avg_latency_ms,
    round(((100.0 * sum(cost_usd)) / NULLIF(( SELECT sum(ai_requests_1.cost_usd) AS sum FROM ai_requests ai_requests_1), (0)::numeric)), 1) AS share_pct
   FROM ai_requests GROUP BY request_type
  ORDER BY (round((sum(cost_usd) * (95)::numeric), 2)) DESC;

CREATE OR REPLACE VIEW analytics.v_ai_costs_monthly AS
 SELECT (date_trunc('month'::text, created_at))::date AS month, request_type,
    count(*) AS requests_count, count(DISTINCT user_id) AS unique_users,
    sum(input_tokens) AS input_tokens, sum(output_tokens) AS output_tokens, sum(total_tokens) AS total_tokens,
    round(sum(cost_usd), 4) AS cost_usd, round((sum(cost_usd) * (95)::numeric), 2) AS cost_rub,
    round((avg(cost_usd) * (95)::numeric), 4) AS avg_cost_per_request_rub,
    round(avg(latency_ms), 0) AS avg_latency_ms
   FROM ai_requests GROUP BY (date_trunc('month'::text, created_at)), request_type
  ORDER BY ((date_trunc('month'::text, created_at))::date) DESC, request_type LIMIT 100;

CREATE OR REPLACE VIEW analytics.v_ai_costs_total_monthly AS
 SELECT (date_trunc('month'::text, created_at))::date AS month,
    count(*) AS requests_count, count(DISTINCT user_id) AS unique_users,
    sum(total_tokens) AS total_tokens, round(sum(cost_usd), 4) AS cost_usd, round((sum(cost_usd) * (95)::numeric), 2) AS cost_rub
   FROM ai_requests GROUP BY (date_trunc('month'::text, created_at))
  ORDER BY ((date_trunc('month'::text, created_at))::date) DESC LIMIT 100;

CREATE OR REPLACE VIEW analytics.v_ai_questions_per_user_weekly AS
 WITH weekly AS (
         SELECT (date_trunc('week'::text, ai_requests.created_at))::date AS week, ai_requests.user_id, count(*) AS questions
           FROM ai_requests
          WHERE ((ai_requests.request_type = 'qa'::text) AND (ai_requests.created_at >= (now() - '84 days'::interval)))
          GROUP BY ((date_trunc('week'::text, ai_requests.created_at))::date), ai_requests.user_id
        )
 SELECT week, count(DISTINCT user_id) AS active_qa_users, sum(questions) AS total_questions,
    round(avg(questions), 2) AS avg_per_user,
    (percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((questions)::double precision)))::numeric(10,1) AS median_per_user
   FROM weekly GROUP BY week ORDER BY week;

CREATE OR REPLACE VIEW analytics.v_care_actions_summary AS
 SELECT action_type, count(*) AS actions_count, count(DISTINCT user_id) AS unique_users,
    count(DISTINCT plant_id) AS unique_plants,
    round(((count(*))::numeric / (NULLIF(count(DISTINCT user_id), 0))::numeric), 2) AS avg_per_user
   FROM care_history
  WHERE ((action_date >= (now() - '30 days'::interval)) AND (user_id IS NOT NULL))
  GROUP BY action_type ORDER BY (count(*)) DESC;

CREATE OR REPLACE VIEW analytics.v_dau_mau_daily AS
 WITH date_series AS (
         SELECT (generate_series((((CURRENT_DATE - '89 days'::interval))::date)::timestamp with time zone, (CURRENT_DATE)::timestamp with time zone, '1 day'::interval))::date AS d
        )
 SELECT d AS date,
    ( SELECT count(DISTINCT users.user_id) AS count FROM users WHERE ((users.last_activity)::date = ds.d)) AS dau,
    ( SELECT count(DISTINCT users.user_id) AS count FROM users WHERE (((users.last_activity)::date >= (ds.d - '6 days'::interval)) AND ((users.last_activity)::date <= ds.d))) AS wau,
    ( SELECT count(DISTINCT users.user_id) AS count FROM users WHERE (((users.last_activity)::date >= (ds.d - '29 days'::interval)) AND ((users.last_activity)::date <= ds.d))) AS mau
   FROM date_series ds ORDER BY d DESC LIMIT 100;

CREATE OR REPLACE VIEW analytics.v_plan_switching AS
 SELECT old_plan_id AS from_plan, new_plan_id AS to_plan, count(*) AS transitions, count(DISTINCT user_id) AS unique_users
   FROM subscription_events
  WHERE ((event_type = ANY (ARRAY['upgraded'::text, 'downgraded'::text])) AND (created_at >= (now() - '90 days'::interval)) AND (old_plan_id IS NOT NULL) AND (new_plan_id IS NOT NULL))
  GROUP BY old_plan_id, new_plan_id ORDER BY old_plan_id, new_plan_id;

CREATE OR REPLACE VIEW analytics.v_plants_by_state AS
 SELECT COALESCE(current_state, 'unknown'::text) AS state, count(*) AS plants_count,
    round(((100.0 * (count(*))::numeric) / (NULLIF(( SELECT count(*) AS count FROM plants plants_1), 0))::numeric), 1) AS share_pct
   FROM plants GROUP BY current_state ORDER BY (count(*)) DESC;

CREATE OR REPLACE VIEW analytics.v_plants_per_user_distribution AS
 WITH user_plants AS (
         SELECT u.user_id, count(p.id) AS plants_count
           FROM (users u LEFT JOIN plants p ON ((p.user_id = u.user_id))) GROUP BY u.user_id
        ), buckets AS (
         SELECT t.bucket_order, t.bucket_label
           FROM ( VALUES (1,'0 растений'::text), (2,'1 растение'::text), (3,'2-3 растения'::text), (4,'4-7 растений'::text), (5,'8-15 растений'::text), (6,'16+ растений'::text)) t(bucket_order, bucket_label)
        ), classified AS (
         SELECT
                CASE
                    WHEN (user_plants.plants_count = 0) THEN 1
                    WHEN (user_plants.plants_count = 1) THEN 2
                    WHEN (user_plants.plants_count <= 3) THEN 3
                    WHEN (user_plants.plants_count <= 7) THEN 4
                    WHEN (user_plants.plants_count <= 15) THEN 5
                    ELSE 6
                END AS bucket_order
           FROM user_plants
        )
 SELECT b.bucket_order, b.bucket_label, COALESCE(count(c.bucket_order), (0)::bigint) AS count
   FROM (buckets b LEFT JOIN classified c ON ((c.bucket_order = b.bucket_order)))
  GROUP BY b.bucket_order, b.bucket_label ORDER BY b.bucket_order;

CREATE OR REPLACE VIEW analytics.v_plants_per_user_stats AS
 WITH user_plants AS (
         SELECT u.user_id, count(p.id) AS plants_count
           FROM (users u LEFT JOIN plants p ON ((p.user_id = u.user_id))) GROUP BY u.user_id
        )
 SELECT count(*) AS total_users, round(avg(plants_count), 2) AS avg_plants,
    (percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((plants_count)::double precision)))::numeric(10,1) AS median_plants,
    max(plants_count) AS max_plants, count(*) FILTER (WHERE (plants_count = 0)) AS users_with_no_plants,
    round(((100.0 * (count(*) FILTER (WHERE (plants_count = 0)))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS pct_with_no_plants
   FROM user_plants;

CREATE OR REPLACE VIEW analytics.v_photos_per_user AS
 WITH photos AS (
         SELECT u.user_id, ( SELECT count(*) AS count FROM plant_analyses_full pa WHERE (pa.user_id = u.user_id)) AS photos_count
           FROM users u
        )
 SELECT count(*) AS total_users, round(avg(photos_count), 2) AS avg_photos,
    (percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((photos_count)::double precision)))::numeric(10,1) AS median_photos,
    max(photos_count) AS max_photos, count(*) FILTER (WHERE (photos_count = 0)) AS users_with_no_photos,
    round(((100.0 * (count(*) FILTER (WHERE (photos_count = 0)))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS pct_with_no_photos
   FROM photos;

CREATE OR REPLACE VIEW analytics.v_streak_distribution AS
 WITH buckets AS (
         SELECT t.bucket_order, t.bucket_label
           FROM ( VALUES (1,'0 (нет streak)'::text), (2,'1-3 дня'::text), (3,'4-7 дней'::text), (4,'8-14 дней'::text), (5,'15-30 дней'::text), (6,'31-90 дней'::text), (7,'90+ дней'::text)) t(bucket_order, bucket_label)
        ), classified AS (
         SELECT
                CASE
                    WHEN (COALESCE(plants.current_streak, 0) = 0) THEN 1
                    WHEN (plants.current_streak <= 3) THEN 2
                    WHEN (plants.current_streak <= 7) THEN 3
                    WHEN (plants.current_streak <= 14) THEN 4
                    WHEN (plants.current_streak <= 30) THEN 5
                    WHEN (plants.current_streak <= 90) THEN 6
                    ELSE 7
                END AS bucket_order
           FROM plants
        )
 SELECT b.bucket_order, b.bucket_label, COALESCE(count(c.bucket_order), (0)::bigint) AS count
   FROM (buckets b LEFT JOIN classified c ON ((c.bucket_order = b.bucket_order)))
  GROUP BY b.bucket_order, b.bucket_label ORDER BY b.bucket_order;

CREATE OR REPLACE VIEW analytics.v_streak_summary AS
 WITH plant_streaks AS (
         SELECT plants.id AS plant_id, plants.user_id,
            COALESCE(plants.current_streak, 0) AS current_streak, COALESCE(plants.max_streak, 0) AS max_streak
           FROM plants
        )
 SELECT count(*) AS total_plants, count(*) FILTER (WHERE (current_streak > 0)) AS plants_with_active_streak,
    round(((100.0 * (count(*) FILTER (WHERE (current_streak > 0)))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS pct_with_active_streak,
    count(*) FILTER (WHERE (current_streak >= 7)) AS plants_streak_7plus,
    round(((100.0 * (count(*) FILTER (WHERE (current_streak >= 7)))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS pct_streak_7plus,
    count(*) FILTER (WHERE (current_streak >= 30)) AS plants_streak_30plus,
    round(((100.0 * (count(*) FILTER (WHERE (current_streak >= 30)))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS pct_streak_30plus,
    round(avg(current_streak), 1) AS avg_current_streak,
    (percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((current_streak)::double precision)))::numeric(10,1) AS median_current_streak,
    max(current_streak) AS max_current_streak, max(max_streak) AS all_time_max_streak, round(avg(max_streak), 1) AS avg_max_streak
   FROM plant_streaks;

CREATE OR REPLACE VIEW analytics.v_top_streak_users AS
 SELECT user_id, count(*) AS plants_count,
    max(COALESCE(current_streak, 0)) AS best_current_streak, max(COALESCE(max_streak, 0)) AS best_max_streak,
    round(avg(COALESCE(current_streak, 0)), 1) AS avg_current_streak,
    count(*) FILTER (WHERE (current_streak > 0)) AS plants_active_now
   FROM plants GROUP BY user_id
 HAVING (max(COALESCE(max_streak, 0)) > 0)
  ORDER BY (max(COALESCE(max_streak, 0))) DESC, (max(COALESCE(current_streak, 0))) DESC LIMIT 15;

CREATE OR REPLACE VIEW analytics.v_reactivation AS
 WITH historical_churns AS (
         SELECT se.user_id, se.created_at AS churn_event_at
           FROM subscription_events se
          WHERE (se.event_type = ANY (ARRAY['payment_failed'::text, 'auto_pay_disabled'::text, 'cancelled'::text]))
        ), ranked_churns AS (
         SELECT h.user_id, h.churn_event_at, min(p.created_at) AS reactivated_at
           FROM (historical_churns h
             LEFT JOIN payments p ON (((p.user_id = h.user_id) AND (p.status = 'succeeded'::text) AND (p.created_at > (h.churn_event_at + '1 day'::interval)))))
          GROUP BY h.user_id, h.churn_event_at
        ), classified AS (
         SELECT ranked_churns.user_id, ranked_churns.churn_event_at, ranked_churns.reactivated_at,
                CASE
                    WHEN (ranked_churns.reactivated_at IS NULL) THEN NULL::numeric
                    ELSE (EXTRACT(epoch FROM (ranked_churns.reactivated_at - ranked_churns.churn_event_at)) / (86400)::numeric)
                END AS days_gap
           FROM ranked_churns
          WHERE (ranked_churns.churn_event_at <= (now() - '30 days'::interval))
        )
 SELECT count(*) AS total_churn_events,
    count(*) FILTER (WHERE ((days_gap IS NOT NULL) AND (days_gap <= (30)::numeric))) AS reactivated_30d,
    count(*) FILTER (WHERE ((days_gap IS NOT NULL) AND (days_gap <= (60)::numeric))) AS reactivated_60d,
    count(*) FILTER (WHERE ((days_gap IS NOT NULL) AND (days_gap <= (90)::numeric))) AS reactivated_90d,
    round(((100.0 * (count(*) FILTER (WHERE ((days_gap IS NOT NULL) AND (days_gap <= (30)::numeric))))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS rate_30d_pct,
    round(((100.0 * (count(*) FILTER (WHERE ((days_gap IS NOT NULL) AND (days_gap <= (60)::numeric))))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS rate_60d_pct,
    round(((100.0 * (count(*) FILTER (WHERE ((days_gap IS NOT NULL) AND (days_gap <= (90)::numeric))))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS rate_90d_pct
   FROM classified;

CREATE OR REPLACE VIEW analytics.v_refund_rate_monthly AS
 WITH months AS (
         SELECT (generate_series((date_trunc('month'::text, now()) - '11 mons'::interval), date_trunc('month'::text, now()), '1 mon'::interval))::date AS month
        ), payments_count AS (
         SELECT (date_trunc('month'::text, yookassa_webhooks_log.received_at))::date AS month, count(*) AS cnt
           FROM yookassa_webhooks_log
          WHERE ((yookassa_webhooks_log.event_type = 'payment.succeeded'::text) AND (yookassa_webhooks_log.received_at >= (now() - '1 year'::interval)))
          GROUP BY ((date_trunc('month'::text, yookassa_webhooks_log.received_at))::date)
        ), refunds_count AS (
         SELECT (date_trunc('month'::text, yookassa_webhooks_log.received_at))::date AS month, count(*) AS cnt
           FROM yookassa_webhooks_log
          WHERE ((yookassa_webhooks_log.event_type = 'refund.succeeded'::text) AND (yookassa_webhooks_log.received_at >= (now() - '1 year'::interval)))
          GROUP BY ((date_trunc('month'::text, yookassa_webhooks_log.received_at))::date)
        )
 SELECT m.month, COALESCE(p.cnt, (0)::bigint) AS payments, COALESCE(r.cnt, (0)::bigint) AS refunds,
    round(((100.0 * (COALESCE(r.cnt, (0)::bigint))::numeric) / (NULLIF(COALESCE(p.cnt, (0)::bigint), 0))::numeric), 2) AS refund_rate_pct
   FROM ((months m LEFT JOIN payments_count p ON ((p.month = m.month))) LEFT JOIN refunds_count r ON ((r.month = m.month)))
  ORDER BY m.month;

CREATE OR REPLACE VIEW analytics.v_failed_payment_rate_monthly AS
 WITH months AS (
         SELECT (generate_series((date_trunc('month'::text, now()) - '11 mons'::interval), date_trunc('month'::text, now()), '1 mon'::interval))::date AS month
        ), recurring_success AS (
         SELECT (date_trunc('month'::text, se.created_at))::date AS month, count(*) AS cnt
           FROM subscription_events se
          WHERE ((se.event_type = 'renewed'::text) AND (COALESCE(se.source, ''::text) ~~ '%yookassa%'::text) AND (se.created_at >= (now() - '1 year'::interval)))
          GROUP BY ((date_trunc('month'::text, se.created_at))::date)
        ), recurring_failed AS (
         SELECT (date_trunc('month'::text, se.created_at))::date AS month, count(*) AS cnt
           FROM subscription_events se
          WHERE ((se.event_type = 'payment_failed'::text) AND (se.created_at >= (now() - '1 year'::interval)))
          GROUP BY ((date_trunc('month'::text, se.created_at))::date)
        )
 SELECT m.month, COALESCE(s.cnt, (0)::bigint) AS success, COALESCE(f.cnt, (0)::bigint) AS failed,
    round(((100.0 * (COALESCE(f.cnt, (0)::bigint))::numeric) / (NULLIF((COALESCE(s.cnt, (0)::bigint) + COALESCE(f.cnt, (0)::bigint)), 0))::numeric), 2) AS failed_rate_pct
   FROM ((months m LEFT JOIN recurring_success s ON ((s.month = m.month))) LEFT JOIN recurring_failed f ON ((f.month = m.month)))
  ORDER BY m.month;

-- ============================================================
-- СЛОЙ 2: зависят от слоя 1
-- ============================================================

CREATE OR REPLACE VIEW analytics.v_user_economics AS
 SELECT u.user_id, u.created_at AS user_created_at, u.last_activity, u.utm_source,
    COALESCE(rev.total_revenue_rub, (0)::bigint) AS lifetime_revenue_rub,
    COALESCE(rev.payments_count, (0)::bigint) AS lifetime_payments,
    COALESCE(ai.total_cost_usd, (0)::numeric) AS lifetime_ai_cost_usd,
    round((COALESCE(ai.total_cost_usd, (0)::numeric) * (95)::numeric), 2) AS lifetime_ai_cost_rub,
    COALESCE(ai.requests_count, (0)::bigint) AS lifetime_ai_requests,
    round(((COALESCE(rev.total_revenue_rub, (0)::bigint))::numeric - (COALESCE(ai.total_cost_usd, (0)::numeric) * (95)::numeric)), 2) AS gross_margin_rub,
    s.plan AS current_plan, s.plan_id AS current_plan_id, s.expires_at AS current_expires_at,
        CASE
            WHEN ((s.plan = 'pro'::text) AND ((s.expires_at + '3 days'::interval) > now()) AND (s.granted_by_admin IS NULL)) THEN true
            ELSE false
        END AS is_currently_paying
   FROM (((users u
     LEFT JOIN ( SELECT v_payments_clean.user_id, sum(v_payments_clean.amount_rub) AS total_revenue_rub, count(*) AS payments_count
           FROM analytics.v_payments_clean GROUP BY v_payments_clean.user_id) rev ON ((rev.user_id = u.user_id)))
     LEFT JOIN ( SELECT ai_requests.user_id, sum(ai_requests.cost_usd) AS total_cost_usd, count(*) AS requests_count
           FROM ai_requests GROUP BY ai_requests.user_id) ai ON ((ai.user_id = u.user_id)))
     LEFT JOIN subscriptions s ON ((s.user_id = u.user_id)))
 LIMIT 100;

CREATE OR REPLACE VIEW analytics.v_mrr_current AS
 SELECT count(*) AS active_subscriptions, round(sum(mrr_rub), 2) AS mrr_rub,
    round((sum(mrr_rub) * (12)::numeric), 2) AS arr_rub, round(avg(mrr_rub), 2) AS arpu_rub
   FROM analytics.v_active_subscriptions
  WHERE (status = ANY (ARRAY['active'::text, 'grace'::text])) LIMIT 100;

CREATE OR REPLACE VIEW analytics.v_subscriptions_by_plan AS
 SELECT s.plan_id, pl.label AS plan_label, pl.days AS plan_days, count(*) AS active_count,
    round(sum(s.mrr_rub), 2) AS mrr_rub, round(avg(s.mrr_rub), 2) AS arpu_rub,
    sum( CASE WHEN s.has_auto_pay THEN 1 ELSE 0 END) AS with_auto_pay
   FROM (analytics.v_active_subscriptions s LEFT JOIN analytics.v_plans pl ON ((pl.plan_id = (s.plan_id)::text)))
  WHERE (s.status = ANY (ARRAY['active'::text, 'grace'::text]))
  GROUP BY s.plan_id, pl.label, pl.days LIMIT 100;

CREATE OR REPLACE VIEW analytics.v_revenue_monthly AS
 SELECT (date_trunc('month'::text, created_at))::date AS month, count(*) AS payments_count,
    count(DISTINCT user_id) AS unique_payers, sum(amount_rub) AS revenue_rub,
    sum( CASE WHEN is_recurring THEN amount_rub ELSE 0 END) AS recurring_revenue_rub,
    sum( CASE WHEN (NOT is_recurring) THEN amount_rub ELSE 0 END) AS new_revenue_rub,
    sum( CASE WHEN is_first_payment THEN amount_rub ELSE 0 END) AS first_payment_revenue_rub
   FROM analytics.v_payments_clean
  GROUP BY (date_trunc('month'::text, created_at))
  ORDER BY ((date_trunc('month'::text, created_at))::date) DESC LIMIT 100;

CREATE OR REPLACE VIEW analytics.v_signup_to_paid AS
 SELECT (date_trunc('week'::text, u.created_at))::date AS signup_week, count(*) AS signups,
    count(DISTINCT p.user_id) AS converted_to_paid,
    round((((count(DISTINCT p.user_id))::numeric * (100)::numeric) / (NULLIF(count(*), 0))::numeric), 2) AS conversion_rate_pct,
    COALESCE(sum(p.amount_rub), (0)::bigint) AS cohort_revenue_rub
   FROM (users u LEFT JOIN analytics.v_payments_clean p ON ((p.user_id = u.user_id)))
  GROUP BY (date_trunc('week'::text, u.created_at))
  ORDER BY ((date_trunc('week'::text, u.created_at))::date) DESC LIMIT 100;

CREATE OR REPLACE VIEW analytics.v_churn_summary AS
 SELECT count(*) AS total_churned,
    count(*) FILTER (WHERE (churn_type = 'voluntary'::text)) AS voluntary,
    count(*) FILTER (WHERE (churn_type = 'involuntary'::text)) AS involuntary,
    count(*) FILTER (WHERE (churn_type = 'admin_revoked'::text)) AS admin_revoked,
    count(*) FILTER (WHERE (churned_at >= (now() - '30 days'::interval))) AS churned_30d,
    (COALESCE(avg(days_to_churn), (0)::numeric))::integer AS avg_days_to_churn,
    (COALESCE(percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((days_to_churn)::double precision)), (0)::double precision))::integer AS median_days_to_churn,
    round(((100.0 * (count(*) FILTER (WHERE (churn_type = 'involuntary'::text)))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS involuntary_pct
   FROM analytics.v_churned_users;

CREATE OR REPLACE VIEW analytics.v_churn_by_month AS
 WITH months AS (
         SELECT (generate_series((date_trunc('month'::text, now()) - '11 mons'::interval), date_trunc('month'::text, now()), '1 mon'::interval))::date AS month
        ), agg AS (
         SELECT (date_trunc('month'::text, v_churned_users.churned_at))::date AS month,
            count(*) FILTER (WHERE (v_churned_users.churn_type = 'voluntary'::text)) AS voluntary,
            count(*) FILTER (WHERE (v_churned_users.churn_type = 'involuntary'::text)) AS involuntary,
            count(*) FILTER (WHERE (v_churned_users.churn_type = 'admin_revoked'::text)) AS admin_revoked
           FROM analytics.v_churned_users
          GROUP BY ((date_trunc('month'::text, v_churned_users.churned_at))::date)
        )
 SELECT m.month, COALESCE(a.voluntary, (0)::bigint) AS voluntary, COALESCE(a.involuntary, (0)::bigint) AS involuntary,
    COALESCE(a.admin_revoked, (0)::bigint) AS admin_revoked,
    ((COALESCE(a.voluntary, (0)::bigint) + COALESCE(a.involuntary, (0)::bigint)) + COALESCE(a.admin_revoked, (0)::bigint)) AS total
   FROM (months m LEFT JOIN agg a ON ((a.month = m.month))) ORDER BY m.month;

CREATE OR REPLACE VIEW analytics.v_days_to_churn_distribution AS
 WITH buckets AS (
         SELECT t.bucket_order, t.bucket_label
           FROM ( VALUES (1,'0-7 дней'::text), (2,'7-30 дней'::text), (3,'30-60 дней'::text), (4,'60-90 дней'::text), (5,'90-180 дней'::text), (6,'180+ дней'::text)) t(bucket_order, bucket_label)
        ), classified AS (
         SELECT
                CASE
                    WHEN (v_churned_users.days_to_churn < 7) THEN 1
                    WHEN (v_churned_users.days_to_churn < 30) THEN 2
                    WHEN (v_churned_users.days_to_churn < 60) THEN 3
                    WHEN (v_churned_users.days_to_churn < 90) THEN 4
                    WHEN (v_churned_users.days_to_churn < 180) THEN 5
                    ELSE 6
                END AS bucket_order
           FROM analytics.v_churned_users
        )
 SELECT b.bucket_order, b.bucket_label, COALESCE(count(c.bucket_order), (0)::bigint) AS count
   FROM (buckets b LEFT JOIN classified c ON ((c.bucket_order = b.bucket_order)))
  GROUP BY b.bucket_order, b.bucket_label ORDER BY b.bucket_order;

CREATE OR REPLACE VIEW analytics.v_heavy_users_extended AS
 WITH user_totals AS (
         SELECT ai_requests.user_id, count(*) AS total_requests, (sum(ai_requests.cost_usd) * (95)::numeric) AS total_cost_rub
           FROM ai_requests GROUP BY ai_requests.user_id
        ), by_type AS (
         SELECT ai_requests.user_id, ai_requests.request_type, count(*) AS req_count, (sum(ai_requests.cost_usd) * (95)::numeric) AS cost_rub
           FROM ai_requests GROUP BY ai_requests.user_id, ai_requests.request_type
        )
 SELECT ut.user_id, u.created_at AS user_created_at, (ut.total_cost_rub)::numeric(12,2) AS total_cost_rub, ut.total_requests,
    COALESCE(max( CASE WHEN (bt.request_type = 'photo_analysis'::text) THEN bt.req_count ELSE NULL::bigint END), (0)::bigint) AS photo_requests,
    (COALESCE(max( CASE WHEN (bt.request_type = 'photo_analysis'::text) THEN bt.cost_rub ELSE NULL::numeric END), (0)::numeric))::numeric(12,2) AS photo_cost_rub,
    COALESCE(max( CASE WHEN (bt.request_type = 'qa'::text) THEN bt.req_count ELSE NULL::bigint END), (0)::bigint) AS qa_requests,
    (COALESCE(max( CASE WHEN (bt.request_type = 'qa'::text) THEN bt.cost_rub ELSE NULL::numeric END), (0)::numeric))::numeric(12,2) AS qa_cost_rub,
    COALESCE(max( CASE WHEN (bt.request_type = 'growing_plan'::text) THEN bt.req_count ELSE NULL::bigint END), (0)::bigint) AS growing_requests,
    (COALESCE(max( CASE WHEN (bt.request_type = 'growing_plan'::text) THEN bt.cost_rub ELSE NULL::numeric END), (0)::numeric))::numeric(12,2) AS growing_cost_rub,
    ue.lifetime_revenue_rub, ue.gross_margin_rub
   FROM (((user_totals ut JOIN users u ON ((u.user_id = ut.user_id)))
     LEFT JOIN by_type bt ON ((bt.user_id = ut.user_id)))
     LEFT JOIN analytics.v_user_economics ue ON ((ue.user_id = ut.user_id)))
  GROUP BY ut.user_id, u.created_at, ut.total_cost_rub, ut.total_requests, ue.lifetime_revenue_rub, ue.gross_margin_rub
  ORDER BY ut.total_cost_rub DESC LIMIT 20;

CREATE OR REPLACE VIEW analytics.v_margin_distribution AS
 WITH buckets AS (
         SELECT t.bucket_order, t.bucket_label
           FROM ( VALUES (1,'Убыточные (< 0₽)'::text), (2,'0 - 100₽'::text), (3,'100 - 500₽'::text), (4,'500 - 1000₽'::text), (5,'1000 - 5000₽'::text), (6,'5000₽+'::text)) t(bucket_order, bucket_label)
        ), classified AS (
         SELECT
                CASE
                    WHEN (v_user_economics.gross_margin_rub < (0)::numeric) THEN 1
                    WHEN (v_user_economics.gross_margin_rub < (100)::numeric) THEN 2
                    WHEN (v_user_economics.gross_margin_rub < (500)::numeric) THEN 3
                    WHEN (v_user_economics.gross_margin_rub < (1000)::numeric) THEN 4
                    WHEN (v_user_economics.gross_margin_rub < (5000)::numeric) THEN 5
                    ELSE 6
                END AS bucket_order
           FROM analytics.v_user_economics
          WHERE ((v_user_economics.lifetime_ai_requests > 0) OR (v_user_economics.lifetime_payments > 0))
        )
 SELECT b.bucket_order, b.bucket_label, COALESCE(count(c.bucket_order), (0)::bigint) AS count
   FROM (buckets b LEFT JOIN classified c ON ((c.bucket_order = b.bucket_order)))
  GROUP BY b.bucket_order, b.bucket_label ORDER BY b.bucket_order;

CREATE OR REPLACE VIEW analytics.v_unit_econ_summary AS
 WITH active_users AS (
         SELECT users.user_id FROM users WHERE (users.last_activity >= (now() - '30 days'::interval))
        ), ai_cost_30d AS (
         SELECT ai_requests.user_id, (sum(ai_requests.cost_usd) * (95)::numeric) AS cost_rub
           FROM ai_requests WHERE (ai_requests.created_at >= (now() - '30 days'::interval)) GROUP BY ai_requests.user_id
        ), per_user_margin AS (
         SELECT ue.user_id, COALESCE(ue.gross_margin_rub, (0)::numeric) AS margin_rub FROM analytics.v_user_economics ue
        )
 SELECT ( SELECT count(*) AS count FROM active_users) AS mau,
    (COALESCE((( SELECT sum(ai_cost_30d.cost_rub) AS sum FROM ai_cost_30d) / (NULLIF(( SELECT count(*) AS count FROM active_users), 0))::numeric), (0)::numeric))::numeric(12,2) AS avg_ai_cost_per_active_user,
    (COALESCE(( SELECT percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((ai_cost_30d.cost_rub)::double precision)) AS percentile_cont FROM ai_cost_30d), (0)::double precision))::numeric(12,2) AS median_ai_cost_per_active_user,
    ( SELECT count(*) AS count FROM per_user_margin WHERE (per_user_margin.margin_rub < (0)::numeric)) AS unit_negative_users,
    ( SELECT count(*) AS count FROM per_user_margin) AS total_users_with_econ,
    round(((100.0 * (( SELECT count(*) AS count FROM per_user_margin WHERE (per_user_margin.margin_rub < (0)::numeric)))::numeric) / (NULLIF(( SELECT count(*) AS count FROM per_user_margin), 0))::numeric), 1) AS unit_negative_pct;

CREATE OR REPLACE VIEW analytics.v_ltv_by_cohort AS
 WITH cohorts AS (
         SELECT users.user_id, (date_trunc('month'::text, users.created_at))::date AS cohort_month
           FROM users WHERE (users.created_at >= (now() - '1 year'::interval))
        ), cohort_sizes AS (
         SELECT cohorts.cohort_month, count(*) AS cohort_size FROM cohorts GROUP BY cohorts.cohort_month
        ), payments_by_life_month AS (
         SELECT c.cohort_month,
            (((EXTRACT(year FROM age(p.created_at, (c.cohort_month)::timestamp without time zone)))::integer * 12) + (EXTRACT(month FROM age(p.created_at, (c.cohort_month)::timestamp without time zone)))::integer) AS life_month,
            (sum(p.amount))::numeric AS revenue_rub
           FROM (cohorts c JOIN payments p ON (((p.user_id = c.user_id) AND (p.status = 'succeeded'::text))))
          WHERE (p.created_at >= c.cohort_month)
          GROUP BY c.cohort_month, (((EXTRACT(year FROM age(p.created_at, (c.cohort_month)::timestamp without time zone)))::integer * 12) + (EXTRACT(month FROM age(p.created_at, (c.cohort_month)::timestamp without time zone)))::integer)
        ), cumulative AS (
         SELECT payments_by_life_month.cohort_month, payments_by_life_month.life_month,
            sum(payments_by_life_month.revenue_rub) OVER (PARTITION BY payments_by_life_month.cohort_month ORDER BY payments_by_life_month.life_month) AS cumulative_revenue_rub
           FROM payments_by_life_month
        )
 SELECT cu.cohort_month, cs.cohort_size, cu.life_month, cu.cumulative_revenue_rub,
    round((cu.cumulative_revenue_rub / (NULLIF(cs.cohort_size, 0))::numeric), 2) AS ltv_per_user_rub
   FROM (cumulative cu JOIN cohort_sizes cs ON ((cs.cohort_month = cu.cohort_month)))
  WHERE (cu.life_month <= 12) ORDER BY cu.cohort_month, cu.life_month;

CREATE OR REPLACE VIEW analytics.v_payback_by_cohort AS
 WITH cohorts AS (
         SELECT users.user_id, (date_trunc('month'::text, users.created_at))::date AS cohort_month, users.created_at AS signup_at
           FROM users WHERE (users.created_at >= (now() - '1 year'::interval))
        ), user_economics AS (
         SELECT c.user_id, c.cohort_month, c.signup_at,
            COALESCE(p.revenue, (0)::numeric) AS revenue, COALESCE(a.ai_cost, (0)::numeric) AS ai_cost,
                CASE
                    WHEN ((COALESCE(p.first_payment_at, NULL::timestamp without time zone) IS NOT NULL) AND (COALESCE(p.revenue, (0)::numeric) >= COALESCE(a.ai_cost, (0)::numeric))) THEN (EXTRACT(epoch FROM (p.first_payment_at - c.signup_at)) / (86400)::numeric)
                    ELSE NULL::numeric
                END AS payback_days
           FROM ((cohorts c
             LEFT JOIN ( SELECT payments.user_id, (sum(payments.amount))::numeric AS revenue, min(payments.created_at) AS first_payment_at
                   FROM payments WHERE (payments.status = 'succeeded'::text) GROUP BY payments.user_id) p ON ((p.user_id = c.user_id)))
             LEFT JOIN ( SELECT ai_requests.user_id, (sum(ai_requests.cost_usd) * (95)::numeric) AS ai_cost
                   FROM ai_requests GROUP BY ai_requests.user_id) a ON ((a.user_id = c.user_id)))
        )
 SELECT cohort_month, count(*) AS cohort_size,
    count(*) FILTER (WHERE (payback_days IS NOT NULL)) AS paid_back_users,
    round(((100.0 * (count(*) FILTER (WHERE (payback_days IS NOT NULL)))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS payback_rate_pct,
    round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((payback_days)::double precision)))::numeric, 1) AS median_payback_days,
    round(avg(payback_days), 1) AS avg_payback_days
   FROM user_economics GROUP BY cohort_month ORDER BY cohort_month DESC;

CREATE OR REPLACE VIEW analytics.v_cohort_retention_triangle AS
 WITH cohorts AS (
         SELECT users.user_id, (date_trunc('month'::text, users.created_at))::date AS cohort_month
           FROM users WHERE (users.created_at >= (now() - '1 year'::interval))
        ), cohort_sizes AS (
         SELECT cohorts.cohort_month, count(*) AS cohort_size FROM cohorts GROUP BY cohorts.cohort_month
        ), active_in_life_month AS (
         SELECT c.cohort_month,
            (((EXTRACT(year FROM age((am.month)::timestamp with time zone, (c.cohort_month)::timestamp with time zone)))::integer * 12) + (EXTRACT(month FROM age((am.month)::timestamp with time zone, (c.cohort_month)::timestamp with time zone)))::integer) AS life_month,
            count(DISTINCT am.user_id) AS active_users
           FROM (cohorts c JOIN analytics._user_active_months am ON ((am.user_id = c.user_id)))
          WHERE (am.month >= c.cohort_month)
          GROUP BY c.cohort_month, (((EXTRACT(year FROM age((am.month)::timestamp with time zone, (c.cohort_month)::timestamp with time zone)))::integer * 12) + (EXTRACT(month FROM age((am.month)::timestamp with time zone, (c.cohort_month)::timestamp with time zone)))::integer)
        )
 SELECT a.cohort_month, cs.cohort_size, a.life_month, a.active_users,
    round(((100.0 * (a.active_users)::numeric) / (NULLIF(cs.cohort_size, 0))::numeric), 1) AS retention_pct
   FROM (active_in_life_month a JOIN cohort_sizes cs ON ((cs.cohort_month = a.cohort_month)))
  WHERE (a.life_month <= 12) ORDER BY a.cohort_month DESC, a.life_month;

CREATE OR REPLACE VIEW analytics.v_cumulative_cohort_revenue AS
 WITH cohorts AS (
         SELECT users.user_id, (date_trunc('month'::text, users.created_at))::date AS cohort_month
           FROM users WHERE (users.created_at >= (now() - '1 year'::interval))
        ), cohort_sizes AS (
         SELECT cohorts.cohort_month, count(*) AS cohort_size FROM cohorts GROUP BY cohorts.cohort_month
        ), cohort_revenue AS (
         SELECT c.cohort_month, (COALESCE(sum(p.amount), (0)::bigint))::numeric AS total_revenue_rub
           FROM (cohorts c LEFT JOIN payments p ON (((p.user_id = c.user_id) AND (p.status = 'succeeded'::text))))
          GROUP BY c.cohort_month
        )
 SELECT cr.cohort_month, cs.cohort_size, cr.total_revenue_rub,
    round((cr.total_revenue_rub / (NULLIF(cs.cohort_size, 0))::numeric), 2) AS revenue_per_user_rub
   FROM (cohort_revenue cr JOIN cohort_sizes cs ON ((cs.cohort_month = cr.cohort_month)))
  ORDER BY cr.cohort_month DESC;

CREATE OR REPLACE VIEW analytics.v_cohort_dn_retention AS
 WITH cohorts AS (
         SELECT users.user_id, (date_trunc('week'::text, users.created_at))::date AS cohort_week, date(users.created_at) AS signup_day
           FROM users WHERE ((users.created_at >= (now() - '90 days'::interval)) AND (users.created_at <= (now() - '7 days'::interval)))
        ), sizes AS (
         SELECT cohorts.cohort_week, count(*) AS cohort_size FROM cohorts GROUP BY cohorts.cohort_week
        ), retention_data AS (
         SELECT c.cohort_week, c.user_id,
            bool_or(((d.day >= (c.signup_day + 7)) AND (d.day < (c.signup_day + 8)))) AS active_d7,
            bool_or(((d.day >= (c.signup_day + 14)) AND (d.day < (c.signup_day + 15)))) AS active_d14,
            bool_or(((d.day >= (c.signup_day + 28)) AND (d.day < (c.signup_day + 29)))) AS active_d28,
            bool_or(((d.day >= (c.signup_day + 90)) AND (d.day < (c.signup_day + 91)))) AS active_d90
           FROM (cohorts c LEFT JOIN analytics._user_active_days d ON ((d.user_id = c.user_id)))
          GROUP BY c.cohort_week, c.user_id
        )
 SELECT r.cohort_week, s.cohort_size,
    round(((100.0 * (count(*) FILTER (WHERE r.active_d7))::numeric) / (NULLIF(s.cohort_size, 0))::numeric), 1) AS d7_pct,
        CASE WHEN ((r.cohort_week + '14 days'::interval) <= (now())::date) THEN round(((100.0 * (count(*) FILTER (WHERE r.active_d14))::numeric) / (NULLIF(s.cohort_size, 0))::numeric), 1) ELSE NULL::numeric END AS d14_pct,
        CASE WHEN ((r.cohort_week + '28 days'::interval) <= (now())::date) THEN round(((100.0 * (count(*) FILTER (WHERE r.active_d28))::numeric) / (NULLIF(s.cohort_size, 0))::numeric), 1) ELSE NULL::numeric END AS d28_pct,
        CASE WHEN ((r.cohort_week + '90 days'::interval) <= (now())::date) THEN round(((100.0 * (count(*) FILTER (WHERE r.active_d90))::numeric) / (NULLIF(s.cohort_size, 0))::numeric), 1) ELSE NULL::numeric END AS d90_pct
   FROM (retention_data r JOIN sizes s ON ((s.cohort_week = r.cohort_week)))
  GROUP BY r.cohort_week, s.cohort_size ORDER BY r.cohort_week DESC;

CREATE OR REPLACE VIEW analytics.v_subscription_retention_curve AS
 WITH first_payments AS (
         SELECT payments.user_id, min(payments.created_at) AS first_paid_at, (date_trunc('month'::text, min(payments.created_at)))::date AS cohort_month
           FROM payments WHERE (payments.status = 'succeeded'::text) GROUP BY payments.user_id
        ), classified AS (
         SELECT fp.cohort_month, fp.user_id, fp.first_paid_at,
            ( SELECT max(p.created_at) AS max FROM payments p WHERE ((p.user_id = fp.user_id) AND (p.status = 'succeeded'::text))) AS last_paid_at,
            s_1.expires_at, ((s_1.expires_at IS NOT NULL) AND (s_1.expires_at > now())) AS is_currently_active
           FROM (first_payments fp LEFT JOIN subscriptions s_1 ON ((s_1.user_id = fp.user_id)))
        ), sizes AS (
         SELECT first_payments.cohort_month, count(*) AS cohort_size FROM first_payments GROUP BY first_payments.cohort_month
        )
 SELECT c.cohort_month, s.cohort_size,
        CASE WHEN ((c.cohort_month + '30 days'::interval) <= (now())::date) THEN round(((100.0 * (count(*) FILTER (WHERE (((c.expires_at IS NOT NULL) AND (c.expires_at >= (c.first_paid_at + '30 days'::interval))) OR (c.last_paid_at >= (c.first_paid_at + '30 days'::interval)))))::numeric) / (NULLIF(s.cohort_size, 0))::numeric), 1) ELSE NULL::numeric END AS d30_pct,
        CASE WHEN ((c.cohort_month + '60 days'::interval) <= (now())::date) THEN round(((100.0 * (count(*) FILTER (WHERE (((c.expires_at IS NOT NULL) AND (c.expires_at >= (c.first_paid_at + '60 days'::interval))) OR (c.last_paid_at >= (c.first_paid_at + '60 days'::interval)))))::numeric) / (NULLIF(s.cohort_size, 0))::numeric), 1) ELSE NULL::numeric END AS d60_pct,
        CASE WHEN ((c.cohort_month + '90 days'::interval) <= (now())::date) THEN round(((100.0 * (count(*) FILTER (WHERE (((c.expires_at IS NOT NULL) AND (c.expires_at >= (c.first_paid_at + '90 days'::interval))) OR (c.last_paid_at >= (c.first_paid_at + '90 days'::interval)))))::numeric) / (NULLIF(s.cohort_size, 0))::numeric), 1) ELSE NULL::numeric END AS d90_pct,
        CASE WHEN ((c.cohort_month + '180 days'::interval) <= (now())::date) THEN round(((100.0 * (count(*) FILTER (WHERE (((c.expires_at IS NOT NULL) AND (c.expires_at >= (c.first_paid_at + '180 days'::interval))) OR (c.last_paid_at >= (c.first_paid_at + '180 days'::interval)))))::numeric) / (NULLIF(s.cohort_size, 0))::numeric), 1) ELSE NULL::numeric END AS d180_pct
   FROM (classified c JOIN sizes s ON ((s.cohort_month = c.cohort_month)))
  GROUP BY c.cohort_month, s.cohort_size ORDER BY c.cohort_month DESC;

CREATE OR REPLACE VIEW analytics.v_mrr_movement_monthly AS
 WITH months AS (
         SELECT (generate_series((date_trunc('month'::text, now()) - '11 mons'::interval), date_trunc('month'::text, now()), '1 mon'::interval))::date AS month
        ), new_mrr AS (
         SELECT (date_trunc('month'::text, se.created_at))::date AS month,
            sum((((se.amount_rub)::numeric / (NULLIF(pd.days, 0))::numeric) * (30)::numeric)) AS amount
           FROM (subscription_events se LEFT JOIN analytics._plan_days pd ON ((pd.plan_id = (se.new_plan_id)::text)))
          WHERE ((se.event_type = 'created'::text) AND (se.created_at >= (now() - '1 year'::interval)) AND (se.amount_rub IS NOT NULL))
          GROUP BY ((date_trunc('month'::text, se.created_at))::date)
        ), expansion_mrr AS (
         SELECT (date_trunc('month'::text, se.created_at))::date AS month,
            sum((((se.amount_rub)::numeric / (NULLIF(pd.days, 0))::numeric) * (30)::numeric)) AS amount
           FROM (subscription_events se LEFT JOIN analytics._plan_days pd ON ((pd.plan_id = (se.new_plan_id)::text)))
          WHERE ((se.event_type = 'upgraded'::text) AND (se.created_at >= (now() - '1 year'::interval)) AND (se.amount_rub IS NOT NULL))
          GROUP BY ((date_trunc('month'::text, se.created_at))::date)
        ), contraction_mrr AS (
         SELECT (date_trunc('month'::text, se.created_at))::date AS month,
            (- sum((((se.amount_rub)::numeric / (NULLIF(pd.days, 0))::numeric) * (30)::numeric))) AS amount
           FROM (subscription_events se LEFT JOIN analytics._plan_days pd ON ((pd.plan_id = (se.new_plan_id)::text)))
          WHERE ((se.event_type = 'downgraded'::text) AND (se.created_at >= (now() - '1 year'::interval)) AND (se.amount_rub IS NOT NULL))
          GROUP BY ((date_trunc('month'::text, se.created_at))::date)
        ), churn_mrr AS (
         SELECT (date_trunc('month'::text, cu.churned_at))::date AS month,
            (- sum((((p.amount)::numeric / (NULLIF(pd.days, 0))::numeric) * (30)::numeric))) AS amount
           FROM ((analytics.v_churned_users cu
             LEFT JOIN analytics._plan_days pd ON ((pd.plan_id = (cu.last_plan_id)::text)))
             LEFT JOIN LATERAL ( SELECT payments.amount FROM payments
                  WHERE ((payments.user_id = cu.user_id) AND (payments.status = 'succeeded'::text) AND (payments.created_at <= cu.churned_at))
                  ORDER BY payments.created_at DESC LIMIT 1) p ON (true))
          WHERE (cu.churned_at >= (now() - '1 year'::interval))
          GROUP BY ((date_trunc('month'::text, cu.churned_at))::date)
        )
 SELECT m.month,
    (COALESCE(n.amount, (0)::numeric))::numeric(12,2) AS new_mrr,
    (COALESCE(e.amount, (0)::numeric))::numeric(12,2) AS expansion_mrr,
    (COALESCE(c.amount, (0)::numeric))::numeric(12,2) AS contraction_mrr,
    (COALESCE(ch.amount, (0)::numeric))::numeric(12,2) AS churn_mrr,
    ((((COALESCE(n.amount, (0)::numeric) + COALESCE(e.amount, (0)::numeric)) + COALESCE(c.amount, (0)::numeric)) + COALESCE(ch.amount, (0)::numeric)))::numeric(12,2) AS net_mrr_change,
        CASE
            WHEN (abs((COALESCE(c.amount, (0)::numeric) + COALESCE(ch.amount, (0)::numeric))) < 0.01) THEN NULL::numeric
            ELSE round(((COALESCE(n.amount, (0)::numeric) + COALESCE(e.amount, (0)::numeric)) / NULLIF(abs((COALESCE(c.amount, (0)::numeric) + COALESCE(ch.amount, (0)::numeric))), (0)::numeric)), 2)
        END AS quick_ratio
   FROM ((((months m LEFT JOIN new_mrr n ON ((n.month = m.month)))
     LEFT JOIN expansion_mrr e ON ((e.month = m.month)))
     LEFT JOIN contraction_mrr c ON ((c.month = m.month)))
     LEFT JOIN churn_mrr ch ON ((ch.month = m.month)))
  ORDER BY m.month;

-- ============================================================
-- СЛОЙ 3: AARRR и activation (зависят от слоёв 2)
-- ============================================================

CREATE OR REPLACE VIEW analytics.v_activation_funnel_weekly AS
 WITH cohorts AS (
         SELECT users.user_id, (date_trunc('week'::text, users.created_at))::date AS cohort_week, users.created_at AS signup_at
           FROM users WHERE ((users.created_at >= (now() - '56 days'::interval)) AND (users.created_at <= (now() - '7 days'::interval)))
        ), first_actions AS (
         SELECT c.user_id, c.cohort_week, c.signup_at,
            (EXISTS ( SELECT 1 FROM plants p WHERE ((p.user_id = c.user_id) AND (p.saved_date >= c.signup_at) AND (p.saved_date < (c.signup_at + '7 days'::interval))))) AS has_plant_7d,
            (EXISTS ( SELECT 1 FROM plant_analyses_full pa WHERE ((pa.user_id = c.user_id) AND (pa.analysis_date >= c.signup_at) AND (pa.analysis_date < (c.signup_at + '7 days'::interval))))) AS has_photo_7d,
            (EXISTS ( SELECT 1 FROM plant_qa_history qa WHERE ((qa.user_id = c.user_id) AND (qa.question_date >= c.signup_at) AND (qa.question_date < (c.signup_at + '7 days'::interval))))) AS has_qa_7d
           FROM cohorts c
        )
 SELECT cohort_week, count(*) AS signups,
    count(*) FILTER (WHERE has_plant_7d) AS step_plant_added,
    count(*) FILTER (WHERE has_photo_7d) AS step_photo_analysis,
    count(*) FILTER (WHERE (has_plant_7d AND has_photo_7d)) AS step_activated,
    count(*) FILTER (WHERE has_qa_7d) AS step_ai_question,
    round(((100.0 * (count(*) FILTER (WHERE has_plant_7d))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS pct_plant,
    round(((100.0 * (count(*) FILTER (WHERE has_photo_7d))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS pct_photo,
    round(((100.0 * (count(*) FILTER (WHERE (has_plant_7d AND has_photo_7d)))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS pct_activated,
    round(((100.0 * (count(*) FILTER (WHERE has_qa_7d))::numeric) / (NULLIF(count(*), 0))::numeric), 1) AS pct_qa
   FROM first_actions GROUP BY cohort_week ORDER BY cohort_week DESC;

CREATE OR REPLACE VIEW analytics.v_activation_summary AS
 SELECT sum(signups) AS total_signups, sum(step_activated) AS total_activated,
    round(((100.0 * sum(step_activated)) / NULLIF(sum(signups), (0)::numeric)), 1) AS activation_rate_pct,
    ( SELECT v_activation_funnel_weekly_1.pct_activated FROM analytics.v_activation_funnel_weekly v_activation_funnel_weekly_1
          ORDER BY v_activation_funnel_weekly_1.cohort_week DESC LIMIT 1) AS latest_week_pct
   FROM analytics.v_activation_funnel_weekly;

CREATE OR REPLACE VIEW analytics.v_aarrr_acquisition AS
 WITH last_7d AS (
         SELECT (count(*))::integer AS signups_7d FROM users WHERE (users.created_at >= (now() - '7 days'::interval))
        ), prev_7d AS (
         SELECT (count(*))::integer AS signups_prev_7d FROM users WHERE ((users.created_at >= (now() - '14 days'::interval)) AND (users.created_at < (now() - '7 days'::interval)))
        )
 SELECT l.signups_7d,
        CASE
            WHEN ((p.signups_prev_7d = 0) AND (l.signups_7d > 0)) THEN 100.0
            WHEN (p.signups_prev_7d = 0) THEN NULL::numeric
            ELSE round(((100.0 * ((l.signups_7d - p.signups_prev_7d))::numeric) / (p.signups_prev_7d)::numeric), 1)
        END AS trend_pct
   FROM (last_7d l CROSS JOIN prev_7d p);

CREATE OR REPLACE VIEW analytics.v_aarrr_retention AS
 SELECT cohort_week, d28_pct FROM analytics.v_cohort_dn_retention
  WHERE (d28_pct IS NOT NULL) ORDER BY cohort_week DESC LIMIT 1;

CREATE OR REPLACE VIEW analytics.v_aarrr_revenue_trend AS
 WITH this_month AS (
         SELECT COALESCE(sum((v_revenue_monthly.recurring_revenue_rub + v_revenue_monthly.new_revenue_rub)), (0)::numeric) AS mrr_now
           FROM analytics.v_revenue_monthly WHERE (v_revenue_monthly.month = (date_trunc('month'::text, now()))::date)
        ), prev_month AS (
         SELECT COALESCE(sum((v_revenue_monthly.recurring_revenue_rub + v_revenue_monthly.new_revenue_rub)), (0)::numeric) AS mrr_prev
           FROM analytics.v_revenue_monthly WHERE (v_revenue_monthly.month = (date_trunc('month'::text, (now() - '1 mon'::interval)))::date)
        )
 SELECT t.mrr_now AS current_mrr_rub,
        CASE
            WHEN ((p.mrr_prev = (0)::numeric) AND (t.mrr_now > (0)::numeric)) THEN 100.0
            WHEN (p.mrr_prev = (0)::numeric) THEN NULL::numeric
            ELSE round(((100.0 * (t.mrr_now - p.mrr_prev)) / p.mrr_prev), 1)
        END AS trend_pct
   FROM (this_month t CROSS JOIN prev_month p);
