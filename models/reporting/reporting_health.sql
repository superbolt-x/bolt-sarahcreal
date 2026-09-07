{{ config (
    alias = target.database + '_reporting_health'
)}}

/*
    One row per detected reporting problem for Sarah Creal Beauty.
    Pattern mirrors reporting.fabric_reporting_health, adapted for a DTC/ecommerce client
    with no UTM->campaign mapping layer (Fabric's MAPPING_GAP/ORPHAN_UTM checks don't apply
    here — there's no backend-funnel attribution join to break).

    Checks encode two failures found in the 2026-08-21 stack audit:
      STALE_SOURCE            the GA4 Fivetran connector broke 2026-01-13 (OAuth token dead)
                               and reporting.sarahcreal_blended_performance has silently
                               reported near-zero GA4 sessions for 220+ days with no alert
      MISSING_REPORTING_TABLE four models exist in this repo (bingads_campaign_performance,
                               googleads_asset_group_performance, pinterest_pin_performance,
                               facebook_campaign_performance_age) with no corresponding table
                               ever built in the warehouse

    Added 2026-09-07 with the UK market split:
      UNTOLERANCED_CHANNEL    STALE_SOURCE joins channel -> tolerance. That join used to be
                               an inner join, so a NEW channel value silently escaped
                               freshness monitoring entirely. This check makes a channel
                               with no tolerance row a visible failure instead.
      UNKNOWN_MARKET          after routing no-country POS orders to US, the 'unknown' market
                               bucket should always be empty. A row here means an order
                               arrived with no shipping country and no POS source -- i.e. a
                               genuinely new classification gap, not a known leftover.
      UK_OPEN_DATE            informational: the UK open date the pipeline is deriving right
                               now. It is computed, never hardcoded, so this makes the value
                               inspectable rather than implied by a query someone has to
                               write from scratch.

    Severity: 'fail' = needs attention before this data is reported to the client.
                'info' = no action, surfaced so the value is visible.
*/

with

freshness as (
    select
        channel,
        max(date) as max_date
    from {{ ref('blended_performance') }}
    where date_granularity = 'day'
    group by channel
),

-- Per-channel tolerance. Pinterest gets 3d (Fivetran reports it 'connected' but the
-- underlying reporting account has historically lagged a few days); everything else is 2d.
tolerance as (
    select 'Meta' as channel, 2 as tolerance_days
    union all select 'Meta with Traffic', 2
    union all select 'Meta Sephora', 2
    union all select 'Meta UK', 2
    union all select 'Google Ads', 2
    union all select 'Pinterest', 3
    union all select 'Tiktok', 2
    union all select 'Tiktok Sephora', 2
    union all select 'Shopify', 2
    union all select 'GA4', 2
),

stale_sources as (
    select
        'STALE_SOURCE' as check_name,
        'fail' as severity,
        'all' as product,
        f.channel as entity,
        datediff(day, f.max_date, current_date) as metric_value,
        f.channel || ' channel in {{ target.database }}_blended_performance is ' ||
        datediff(day, f.max_date, current_date)::varchar ||
        ' days behind (last date ' || f.max_date::varchar || ', tolerance ' ||
        t.tolerance_days::varchar || 'd).' as detail
    from freshness f
    join tolerance t on t.channel = f.channel
    where datediff(day, f.max_date, current_date) > t.tolerance_days
),

-- A channel present in blended_performance with no row in `tolerance` is not being
-- freshness-checked at all. Adding a channel (e.g. 'Meta UK' for the UK launch) is exactly
-- when this happens, and the failure is invisible: the inner join in stale_sources simply
-- drops it. Surfaced so the next new channel cannot slip through the same way.
untoleranced_channels as (
    select
        'UNTOLERANCED_CHANNEL' as check_name,
        'fail' as severity,
        'all' as product,
        f.channel as entity,
        datediff(day, f.max_date, current_date) as metric_value,
        'Channel ' || f.channel || ' exists in {{ target.database }}_blended_performance ' ||
        'but has no row in the tolerance list in reporting_health.sql, so it is NOT being ' ||
        'freshness-checked. Add it.' as detail
    from freshness f
    left join tolerance t on t.channel = f.channel
    where t.channel is null
),

-- After macros/order_market.sql routes no-country POS orders to US, 'unknown' should be
-- permanently empty. Any order landing here has no shipping country AND is not a POS sale.
unknown_market as (
    select
        'UNKNOWN_MARKET' as check_name,
        'fail' as severity,
        'all' as product,
        'shopify_orders' as entity,
        count(*) as metric_value,
        count(*)::varchar || ' order(s) classified market = ''unknown'' in ' ||
        '{{ target.database }}_blended_performance (most recent ' ||
        max(date)::date::varchar || '). This bucket should be empty: an order with no ' ||
        'shipping country that is also not a POS sale is a new classification gap.' as detail
    from {{ ref('blended_performance') }}
    where date_granularity = 'day'
      and market = 'unknown'
      and shopify_orders > 0
    having count(*) > 0
),

-- The UK open date is derived from the orders, never hardcoded, so that a launch slipping a
-- day cannot silently re-label US sales. Surfaced here so the value in use is inspectable.
-- Note the filters: run against the raw order set this returns 2026-01-15 (comped and
-- marketplace one-offs with subtotal_revenue = 0), not the real 2026-08-31 launch.
uk_open_date as (
    select
        'UK_OPEN_DATE' as check_name,
        'info' as severity,
        'all' as product,
        'UK' as entity,
        0 as metric_value,
        'UK market open date currently derived as ' ||
        coalesce(min(date)::date::varchar, 'NULL - no UK orders yet') ||
        ' (first UK order surviving the reporting filters). Derived, not hardcoded.' as detail
    from {{ ref('shopify_daily_sales_by_order') }}
    where upper(trim(shipping_address_country_code)) in ('GB','UK')
      and cancelled_at is null
      and subtotal_revenue > 0
      and financial_status in ('paid','partially_refunded','refunded','partially_paid')
),

-- Every reporting-layer model that has a .sql file in this project. Compared against what
-- actually exists in the warehouse so a model that was added but never (successfully) run
-- shows up as a warehouse fact instead of being rediscovered by hand during an audit.
expected_tables as (
    {% set expected_models = [
        'facebook_ad_performance',
        'facebook_campaign_performance',
        'facebook_campaign_performance_age',
        'googleads_ad_performance',
        'googleads_asset_group_performance',
        'googleads_campaign_performance',
        'pinterest_ad_group_performance',
        'pinterest_pin_performance',
        'tiktok_ad_performance',
        'bingads_campaign_performance'
    ] %}
    {% for m in expected_models %}
    select '{{ target.database }}_{{ m }}' as expected_table
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

existing_tables as (
    select table_name
    from information_schema.tables
    where table_schema = 'reporting'
),

missing_tables as (
    select
        'MISSING_REPORTING_TABLE' as check_name,
        'fail' as severity,
        'all' as product,
        e.expected_table as entity,
        0 as metric_value,
        'A dbt model file exists for this table in bolt-sarahcreal, but no corresponding ' ||
        'table exists in the reporting schema. dbt has never built it, or the last run ' ||
        'failed silently for this model.' as detail
    from expected_tables e
    left join existing_tables t on t.table_name = e.expected_table
    where t.table_name is null
)

select current_date as checked_on, check_name, severity, product, entity, metric_value, detail from stale_sources
union all
select current_date, check_name, severity, product, entity, metric_value, detail from untoleranced_channels
union all
select current_date, check_name, severity, product, entity, metric_value, detail from unknown_market
union all
select current_date, check_name, severity, product, entity, metric_value, detail from missing_tables
union all
select current_date, check_name, severity, product, entity, metric_value, detail from uk_open_date
