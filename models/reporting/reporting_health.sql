{{ config (
    alias = target.database + '_reporting_health'
)}}

{#
    One row per detected reporting problem for Sarah Creal Beauty.
    Pattern mirrors reporting.fabric_reporting_health, adapted for a DTC/ecommerce client
    with no UTM->campaign mapping layer (Fabric's MAPPING_GAP/ORPHAN_UTM checks don't apply
    here -- there's no backend-funnel attribution join to break).

    Checks:
      STALE_SOURCE            per-channel freshness against blended_performance. Encodes the
                               failure found in the 2026-08-21 audit: the GA4 connector broke
                               2026-01-13 and reported near-zero sessions for 220+ days with
                               no alert.
      UNTOLERANCED_CHANNEL    STALE_SOURCE joins channel -> tolerance. That join is an inner
                               join, so a NEW channel value silently escapes freshness
                               monitoring. This makes a channel with no tolerance row visible.
      UNKNOWN_MARKET          after macros/order_market.sql routes no-country POS orders to
                               US, the 'unknown' market bucket should always be empty. A row
                               here is a genuinely new classification gap.
      STRANDED_ORDERS         orders that fall out of every date bucket -- see the note on
                               the CTE. Pre-existing behaviour, surfaced rather than fixed,
                               because fixing it changes already-reported historical months.
      MISSING_REPORTING_TABLE a model file exists in this repo with no table in the warehouse.
      UK_OPEN_DATE            informational: the UK open date the pipeline derives right now.

    Severity: 'fail' = needs attention before this data is reported to the client.
              'info' = no action, surfaced so the value is visible.

    ---------------------------------------------------------------------------------------
    WHY MISSING_REPORTING_TABLE IS RESOLVED IN JINJA, NOT IN SQL

    information_schema is a leader-node-only relation in Redshift. Combining it with a
    compute-node (user) table in a single statement fails with:

        Specified types or functions (one per INFO message) not supported on Redshift tables.

    That is why this model had never materialized, from its first version onward -- the
    2026-08-21 proposal attributed it to an unknown deploy path, but the model could not
    have built regardless. run_query below executes the catalog lookup as its own statement
    at compile time, so the model body emits plain literals and touches no catalog relation.
    ---------------------------------------------------------------------------------------
#}

{%- set expected_models = [
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
] -%}

{%- set missing_models = [] -%}
{%- if execute -%}
    {%- set catalog = run_query(
        "select table_name from information_schema.tables where table_schema = 'reporting'"
    ) -%}
    {%- set existing = catalog.columns[0].values() | list -%}
    {%- for m in expected_models -%}
        {%- if (target.database ~ '_' ~ m) not in existing -%}
            {%- do missing_models.append(m) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

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
-- 'Meta UK' was added here at the same time as the channel value itself, so the UK launch
-- did not arrive unmonitored -- see UNTOLERANCED_CHANNEL for why that matters.
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
        'STALE_SOURCE'::varchar as check_name,
        'fail'::varchar as severity,
        'all'::varchar as product,
        f.channel::varchar as entity,
        datediff(day, f.max_date, current_date)::bigint as metric_value,
        (f.channel || ' channel in {{ target.database }}_blended_performance is ' ||
         datediff(day, f.max_date, current_date)::varchar ||
         ' days behind (last date ' || f.max_date::varchar || ', tolerance ' ||
         t.tolerance_days::varchar || 'd).')::varchar as detail
    from freshness f
    join tolerance t on t.channel = f.channel
    where datediff(day, f.max_date, current_date) > t.tolerance_days
),

untoleranced_channels as (
    select
        'UNTOLERANCED_CHANNEL'::varchar as check_name,
        'fail'::varchar as severity,
        'all'::varchar as product,
        f.channel::varchar as entity,
        datediff(day, f.max_date, current_date)::bigint as metric_value,
        ('Channel ' || f.channel || ' exists in {{ target.database }}_blended_performance ' ||
         'but has no row in the tolerance list in reporting_health.sql, so it is NOT being ' ||
         'freshness-checked. Add it.')::varchar as detail
    from freshness f
    left join tolerance t on t.channel = f.channel
    where t.channel is null
),

-- After macros/order_market.sql routes no-country POS orders to US, 'unknown' should be
-- permanently empty. Any order landing here has no shipping country AND is not a POS sale.
unknown_market as (
    select
        'UNKNOWN_MARKET'::varchar as check_name,
        'fail'::varchar as severity,
        'all'::varchar as product,
        'shopify_orders'::varchar as entity,
        count(*)::bigint as metric_value,
        (count(*)::varchar || ' row(s) classified market = ''unknown'' in ' ||
         '{{ target.database }}_blended_performance (most recent ' ||
         max(date)::date::varchar || '). This bucket should be empty: an order with no ' ||
         'shipping country that is also not a POS sale is a new classification gap.')::varchar as detail
    from {{ ref('blended_performance') }}
    where date_granularity = 'day'
      and market = 'unknown'
      and shopify_orders > 0
    having count(*) > 0
),

-- Orders that fall out of every date bucket entirely.
--
-- blended_performance sets effective_date = fulfillment_date for any month that is not the
-- current one. An order still unfulfilled when its month rolls over therefore gets a NULL
-- effective_date and disappears from every dated report -- it is not late, it is gone.
--
-- This is PRE-EXISTING behaviour, not introduced by the market split. It is surfaced here
-- rather than fixed because the fix (coalesce(fulfillment_date, date)) would change
-- already-reported historical months -- as of 2026-09-07, 214 US orders and $27,446 of
-- gross revenue, concentrated in January 2026 (162 orders, $20,439). That is a client-facing
-- number change and needs a decision, not a silent correction.
stranded_orders as (
    select
        'STRANDED_ORDERS'::varchar as check_name,
        'fail'::varchar as severity,
        'all'::varchar as product,
        'shopify_orders'::varchar as entity,
        sum(shopify_orders)::bigint as metric_value,
        (sum(shopify_orders)::varchar || ' order(s) worth ' ||
         round(sum(shopify_gross_sales))::varchar ||
         ' are in a NULL-date bucket in {{ target.database }}_blended_performance and appear ' ||
         'in no dated report. Cause: effective_date falls back to fulfillment_date for any ' ||
         'non-current month, so orders unfulfilled at month end have no date. Fix would be ' ||
         'coalesce(fulfillment_date, date) -- changes historical reported months.')::varchar as detail
    from {{ ref('blended_performance') }}
    where date is null
      and date_granularity = 'day'
      and shopify_orders > 0
    having sum(shopify_orders) > 0
),

-- The UK open date is derived from the orders, never hardcoded, so that a launch slipping a
-- day cannot silently re-label US sales. Surfaced here so the value in use is inspectable.
-- Note the filters: run against the raw order set this returns 2026-01-15 (comped and
-- marketplace one-offs with subtotal_revenue = 0), not the real 2026-08-31 launch.
uk_open_date as (
    select
        'UK_OPEN_DATE'::varchar as check_name,
        'info'::varchar as severity,
        'all'::varchar as product,
        'UK'::varchar as entity,
        0::bigint as metric_value,
        ('UK market open date currently derived as ' ||
         coalesce(min(date)::date::varchar, 'NULL - no UK orders yet') ||
         ' (first UK order surviving the reporting filters). Derived, not hardcoded.')::varchar as detail
    from {{ ref('shopify_daily_sales_by_order') }}
    where upper(trim(shipping_address_country_code)) in ('GB','UK')
      and cancelled_at is null
      and subtotal_revenue > 0
      and financial_status in ('paid','partially_refunded','refunded','partially_paid')
),

-- Resolved at compile time -- see the header note. No catalog relation appears here.
missing_tables as (
    {%- if missing_models %}
    {%- for m in missing_models %}
    select
        'MISSING_REPORTING_TABLE'::varchar as check_name,
        'fail'::varchar as severity,
        'all'::varchar as product,
        '{{ target.database }}_{{ m }}'::varchar as entity,
        0::bigint as metric_value,
        ('A dbt model file exists for this table in bolt-sarahcreal, but no corresponding ' ||
         'table exists in the reporting schema. dbt has never built it, or the last run ' ||
         'failed for this model.')::varchar as detail
    {% if not loop.last %}union all{% endif %}
    {%- endfor %}
    {%- else %}
    -- every expected model has a table; emit a typed zero-row relation
    select
        null::varchar as check_name,
        null::varchar as severity,
        null::varchar as product,
        null::varchar as entity,
        0::bigint as metric_value,
        null::varchar as detail
    where false
    {%- endif %}
)

select current_date as checked_on, check_name, severity, product, entity, metric_value, detail from stale_sources
union all
select current_date, check_name, severity, product, entity, metric_value, detail from untoleranced_channels
union all
select current_date, check_name, severity, product, entity, metric_value, detail from unknown_market
union all
select current_date, check_name, severity, product, entity, metric_value, detail from stranded_orders
union all
select current_date, check_name, severity, product, entity, metric_value, detail from missing_tables
union all
select current_date, check_name, severity, product, entity, metric_value, detail from uk_open_date
