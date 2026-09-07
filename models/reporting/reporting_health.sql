{{ config (
    alias = target.database + '_reporting_health'
)}}

{#
    One row per detected reporting problem for Sarah Creal Beauty.

    THE CONTRACT: an EMPTY result means everything passed. That only works if a 'fail' row
    genuinely means "a client-facing number is wrong right now". The first version failed
    that test -- it reported 9 fails, and on review (2026-09-07) almost all of them were
    working as intended: channels stale because they are paused, models with no table
    because the connector was never set up, orders excluded because the client asked for
    revenue to be recognised on fulfilment. A permanently-red gate is a gate nobody reads.

    So severity is now earned:
      'fail' = a client-facing number is wrong, or a classification gap has appeared
      'info' = true, worth knowing, expected -- no action implied

    SOURCE FRESHNESS IS DERIVED, NOT DECLARED
    A channel is only BROKEN if its data is behind WHILE IT IS STILL SPENDING. If spend
    stopped when the data stopped, the channel is dormant and that is correct behaviour.
    This is deliberately self-maintaining: nothing to add to a list when a channel is
    paused, and the moment someone unpauses it, spend resumes -- so a genuine pipe failure
    re-flags on its own. As of 2026-09-07 only 3 of 10 channels are actually spending
    (Meta, Google Ads, Meta UK); the other 7 are dormant by choice.

    The one thing that cannot be derived is a channel with no spend metric (Shopify, GA4).
    Staleness there is real unless someone says otherwise, so those need an explicit
    declaration -- and the declaration is itself checked: PAUSED_SOURCE_ACTIVE fires if a
    source declared paused starts landing data again, so the list cannot rot silently.

    NOTE ON THE PREVIOUS TOLERANCE LIST: it was an inner join, so a channel missing from it
    escaped freshness checking entirely. Replaced with a Jinja default plus overrides, which
    removes that failure mode by construction rather than by adding a check for it.
#}

{#- freshness tolerance in days: default, with per-channel overrides -#}
{%- set default_tolerance = 2 -%}
{%- set tolerance_overrides = {} -%}

{#- channels with no spend metric: staleness is real unless declared paused -#}
{%- set spendless_channels = ['Shopify', 'GA4'] -%}

{#- sources deliberately switched off. Confirmed with the account team 2026-09-07:
    the GA4 Fivetran connector is paused on purpose. Anything added here is checked
    by PAUSED_SOURCE_ACTIVE below, so a stale entry surfaces itself. -#}
{%- set declared_paused = ['GA4'] -%}

{#- MISSING_REPORTING_TABLE is resolved at COMPILE time, in two steps.

    1. information_schema is a leader-node-only relation in Redshift, and combining it with a
       compute-node table in one statement fails with "Specified types or functions ... not
       supported on Redshift tables." That is why this model never built before 2026-09-07.
       run_query runs the catalog lookup as its own statement; the model body emits literals
       and touches no catalog relation.

    2. The list of models to expect is read from the dbt GRAPH rather than hardcoded. The
       previous hardcoded list was the same class of problem as the old per-channel tolerance
       table: it had to be edited by hand, so it drifted. Deleting a model file now removes it
       from this check automatically, and adding one enrols it automatically.

       Reading node.alias (not the model name) matters: it is the actual table name dbt will
       create, so a model missing its `{{ config(alias = ...) }}` block is compared against
       the name it would really land under rather than the one we assume. That is exactly the
       bug facebook_campaign_performance_age had before it was retired.
-#}
{%- set expected_tables = [] -%}
{%- set missing_tables_list = [] -%}
{%- if execute -%}
    {%- for node in graph.nodes.values() -%}
        {%- if node.resource_type == 'model'
               and node.package_name == project_name
               and 'reporting' in node.fqn
               and node.config.materialized != 'ephemeral'
               and node.alias != this.identifier -%}
            {%- do expected_tables.append(node.alias) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- set catalog = run_query(
        "select table_name from information_schema.tables where table_schema = 'reporting'"
    ) -%}
    {%- set existing = catalog.columns[0].values() | list -%}
    {%- for t in expected_tables | sort -%}
        {%- if t not in existing -%}
            {%- do missing_tables_list.append(t) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

with

channel_state as (
    select
        channel,
        max(date) as last_data,
        max(case when spend > 0 then date end) as last_spend,
        sum(case when date >= dateadd(day, -14, current_date) then spend else 0 end) as spend_14d,
        sum(case when date >= dateadd(day, -30, current_date) then spend else 0 end) as spend_30d,
        {#- guard the empty case: `case channel else N end` with no WHEN is invalid SQL -#}
        {%- if tolerance_overrides %}
        case channel
            {%- for ch, d in tolerance_overrides.items() %}
            when '{{ ch }}' then {{ d }}
            {%- endfor %}
            else {{ default_tolerance }}
        end as tolerance_days
        {%- else %}
        {{ default_tolerance }} as tolerance_days
        {%- endif %}
    from {{ ref('blended_performance') }}
    where date_granularity = 'day'
    group by channel
),

classified as (
    select
        channel,
        last_data,
        last_spend,
        spend_14d,
        spend_30d,
        tolerance_days,
        datediff(day, last_data, current_date) as data_behind,
        channel in ({{ "'" ~ spendless_channels | join("','") ~ "'" }}) as is_spendless,
        channel in ({{ "'" ~ declared_paused | join("','") ~ "'" }}) as is_declared_paused
    from channel_state
),

verdict as (
    select *,
        case
            -- a source we were told is off, which has started producing again
            when is_declared_paused and data_behind <= tolerance_days then 'PAUSED_SOURCE_ACTIVE'
            when is_declared_paused                                   then 'PAUSED_SOURCE'
            when data_behind <= tolerance_days and (is_spendless or spend_30d > 0) then 'OK'
            -- no spend metric to reason about, so staleness is taken at face value
            when data_behind > tolerance_days and is_spendless        then 'STALE_SOURCE'
            -- data behind while money is still going out: the pipe is broken
            when data_behind > tolerance_days and spend_14d > 0       then 'STALE_SOURCE'
            -- data stopped when spend stopped, or spend has simply stopped: expected
            else 'DORMANT_CHANNEL'
        end as status
    from classified
),

stale_sources as (
    select
        'STALE_SOURCE'::varchar as check_name,
        'fail'::varchar as severity,
        'all'::varchar as product,
        channel::varchar as entity,
        data_behind::bigint as metric_value,
        (channel || ' is ' || data_behind::varchar || ' days behind (last date ' ||
         last_data::date::varchar || ', tolerance ' || tolerance_days::varchar || 'd)' ||
         case when is_spendless
              then ', and has no spend metric to explain it.'
              else ', while still spending -- $' || round(spend_14d)::varchar ||
                   ' in the last 14 days. Money is going out and the data is not coming in.'
         end)::varchar as detail
    from verdict
    where status = 'STALE_SOURCE'
),

paused_source_active as (
    select
        'PAUSED_SOURCE_ACTIVE'::varchar as check_name,
        'fail'::varchar as severity,
        'all'::varchar as product,
        channel::varchar as entity,
        data_behind::bigint as metric_value,
        (channel || ' is declared paused in reporting_health.sql but is landing data again ' ||
         '(last date ' || last_data::date::varchar || '). Remove it from `declared_paused` ' ||
         'so it is freshness-checked properly.')::varchar as detail
    from verdict
    where status = 'PAUSED_SOURCE_ACTIVE'
),

paused_sources as (
    select
        'PAUSED_SOURCE'::varchar as check_name,
        'info'::varchar as severity,
        'all'::varchar as product,
        channel::varchar as entity,
        data_behind::bigint as metric_value,
        (channel || ' is switched off on purpose -- declared paused in reporting_health.sql. ' ||
         'Last data ' || last_data::date::varchar || ', ' || data_behind::varchar ||
         ' days ago. Any number sourced from it is frozen at that date.')::varchar as detail
    from verdict
    where status = 'PAUSED_SOURCE'
),

dormant_channels as (
    select
        'DORMANT_CHANNEL'::varchar as check_name,
        'info'::varchar as severity,
        'all'::varchar as product,
        channel::varchar as entity,
        coalesce(datediff(day, last_spend, current_date), -1)::bigint as metric_value,
        (channel || ' has no spend in the last 30 days' ||
         coalesce(' (last spend ' || last_spend::date::varchar || ', ' ||
                  datediff(day, last_spend, current_date)::varchar || ' days ago)',
                  ' and no spend on record') ||
         '. It still appears in the reporting cards, contributing zeros. Expected if the ' ||
         'channel is paused; worth a look if it is not.')::varchar as detail
    from verdict
    where status = 'DORMANT_CHANNEL'
       or (status = 'OK' and not is_spendless and spend_30d = 0)
),

-- After macros/order_market.sql routes no-country POS orders to US, 'unknown' should be
-- permanently empty. Any order landing here has no shipping country AND is not a POS sale,
-- which means a genuinely new classification gap. Stays 'fail'.
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

-- Orders that never enter a dated report.
--
-- This is INTENTIONAL, confirmed 2026-09-07: the client asked for revenue to be recognised
-- on fulfilment, so blended_performance uses fulfillment_date as the effective date for any
-- non-current month. An unfulfilled order is therefore pending, not lost -- if it ships, it
-- lands on its fulfilment date and counts from then.
--
-- Kept as 'info' because it is not a reporting fault, but the ages are an ops signal: the
-- oldest is 552 days and 162 of them are January 2026 alone, which looks like a fulfilment
-- backlog or a status-sync gap rather than normal drift. The order-level list is Metabase
-- card 57483.
stranded_orders as (
    select
        'PENDING_FULFILMENT'::varchar as check_name,
        'info'::varchar as severity,
        'all'::varchar as product,
        'shopify_orders'::varchar as entity,
        sum(shopify_orders)::bigint as metric_value,
        (sum(shopify_orders)::varchar || ' past-month order(s) worth ' ||
         round(sum(shopify_gross_sales))::varchar || ' are unfulfilled, so they are not in ' ||
         'any dated report. This is by design -- revenue is recognised on fulfilment. They ' ||
         'will count if they ship. Order-level list: Metabase card 57483.')::varchar as detail
    from {{ ref('blended_performance') }}
    where date is null
      and date_granularity = 'day'
      and shopify_orders > 0
    having sum(shopify_orders) > 0
),

-- The UK open date is derived from the orders, never hardcoded, so that a launch slipping a
-- day cannot silently re-label US sales. Surfaced so the value in use is inspectable.
-- Note the filters: against the raw order set this returns 2026-01-15 (comped and
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

-- 'info', not 'fail': nothing queries these tables, so no client-facing number is wrong.
-- Reviewed 2026-09-07 -- Bing has no connector and there is no live PMax campaign, so two
-- of these are expected indefinitely. Kept visible so the list does not have to be
-- rediscovered by hand during the next audit.
missing_tables as (
    {%- if missing_tables_list %}
    {%- for t in missing_tables_list %}
    select
        'MISSING_REPORTING_TABLE'::varchar as check_name,
        'info'::varchar as severity,
        'all'::varchar as product,
        '{{ t }}'::varchar as entity,
        0::bigint as metric_value,
        ('A dbt model file exists for this table in bolt-sarahcreal, but no table exists in ' ||
         'the reporting schema. Expected where the source is not connected; otherwise the ' ||
         'model has never built successfully.')::varchar as detail
    {% if not loop.last %}union all{% endif %}
    {%- endfor %}
    {%- else %}
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
select current_date, check_name, severity, product, entity, metric_value, detail from paused_source_active
union all
select current_date, check_name, severity, product, entity, metric_value, detail from unknown_market
union all
select current_date, check_name, severity, product, entity, metric_value, detail from paused_sources
union all
select current_date, check_name, severity, product, entity, metric_value, detail from dormant_channels
union all
select current_date, check_name, severity, product, entity, metric_value, detail from stranded_orders
union all
select current_date, check_name, severity, product, entity, metric_value, detail from missing_tables
union all
select current_date, check_name, severity, product, entity, metric_value, detail from uk_open_date
