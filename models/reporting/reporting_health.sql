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
    A channel is only BROKEN if its data is behind WHILE IT IS STILL SWITCHED ON. If spend
    stopped when the data stopped, the channel is dormant and that is correct behaviour.

    "Still switched on" was originally read as "spent something in the last 14 days", which
    misfires for up to 14 days after a channel is paused: the window still contains the
    pre-pause spend, so a paused channel reads as a broken pipe. Google Ads UK was reported
    that way on 2026-09-25. campaign_status now carries that judgement (see
    campaign_status_rows below) and the spend window is only the fallback where no status
    is available -- so the answer comes from whether campaigns are actually on, not from
    how recently money moved.
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

{#- Is any campaign on this channel still switched on?

    Spend and data land in the SAME table here, so a paused campaign and a broken connector
    look identical in blended_performance alone: rows up to day X, nothing after. The
    trailing-14-day spend window below cannot separate them either -- a channel that paused
    11 days ago still shows spend inside a 14-day window. That is exactly how Google Ads UK
    was reported as a broken pipe on 2026-09-25, when its only campaign
    ('SB - PMax - Mixed - UK (with Shopping)') had simply been paused on 2026-09-14.

    campaign_status IS able to tell them apart. Each platform keeps emitting rows for a
    campaign after it stops spending, carrying its status, so the LATEST row per campaign is
    its current state. Observed vocabularies (2026-09-25):
      Meta       campaign_effective_status   ACTIVE / PAUSED / ARCHIVED
      Google     campaign_status             ENABLED / PAUSED
      Pinterest  campaign_status             PAUSED  (ACTIVE not currently present)
      Tiktok     campaign_status             CAMPAIGN_STATUS_DISABLE / null

    A NULL status means unknown, never "off", so it counts as possibly-live: an unknown can
    never silence a real staleness. Same for a channel missing from this CTE entirely -- the
    join coalesces to "assume live", preserving the old behaviour wherever the signal is
    unavailable. The channel expressions below mirror blended_performance's exactly; if the
    channel split changes there, it has to change here too.
-#}
campaign_status_rows as (
    select case when campaign_name ilike '%uk%' then 'Meta UK' else 'Meta' end as channel,
           campaign_name, date, campaign_effective_status = 'ACTIVE' as is_active
    from {{ source('reporting','facebook_campaign_performance') }}
    where date_granularity = 'day' and account = 'DTC' and campaign_name !~* 'traffic'
    union all
    select 'Meta with Traffic', campaign_name, date, campaign_effective_status = 'ACTIVE'
    from {{ source('reporting','facebook_campaign_performance') }}
    where date_granularity = 'day' and account = 'DTC' and campaign_name ~* 'traffic'
    union all
    select 'Meta Sephora', campaign_name, date, campaign_effective_status = 'ACTIVE'
    from {{ source('reporting','facebook_campaign_performance') }}
    where date_granularity = 'day' and account = 'Sephora'
    union all
    select case when campaign_name ilike '%uk%' then 'Google Ads UK' else 'Google Ads' end,
           campaign_name, date, campaign_status = 'ENABLED'
    from {{ source('reporting','googleads_campaign_performance') }}
    where date_granularity = 'day'
    union all
    select 'Pinterest', campaign_name, date, campaign_status = 'ACTIVE'
    from {{ source('reporting','pinterest_ad_group_performance') }}
    where date_granularity = 'day'
    union all
    select case when campaign_id = 1861822514294002 then 'Tiktok Sephora' else 'Tiktok' end,
           campaign_name, date, campaign_status = 'CAMPAIGN_STATUS_ENABLE'
    from {{ source('reporting','tiktok_ad_performance') }}
    where date_granularity = 'day'
),

campaign_status_latest as (
    select channel, campaign_name, is_active
    from (
        select channel, campaign_name, is_active,
               row_number() over (partition by channel, campaign_name order by date desc) as rn
        from campaign_status_rows
    )
    where rn = 1
),

channel_live as (
    select channel,
           {#- coalesce inside the aggregate is what makes unknown count as live -#}
           bool_or(coalesce(is_active, true)) as has_live_campaign
    from campaign_status_latest
    group by channel
),

classified as (
    select
        cs.channel,
        cs.last_data,
        cs.last_spend,
        cs.spend_14d,
        cs.spend_30d,
        cs.tolerance_days,
        datediff(day, cs.last_data, current_date) as data_behind,
        cs.channel in ({{ "'" ~ spendless_channels | join("','") ~ "'" }}) as is_spendless,
        cs.channel in ({{ "'" ~ declared_paused | join("','") ~ "'" }}) as is_declared_paused,
        coalesce(cl.has_live_campaign, true) as has_live_campaign
    from channel_state cs
    left join channel_live cl on cl.channel = cs.channel
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
            -- Spent recently but every campaign is switched off: the channel was turned
            -- off part-way through the 14-day window, so the spend below is from BEFORE
            -- the pause. Data stopped because spend stopped. Not a broken pipe. Kept
            -- separate from DORMANT_CHANNEL because that one's wording ("no spend in the
            -- last 30 days") would be false here.
            when data_behind > tolerance_days and spend_14d > 0
                 and not has_live_campaign                            then 'CHANNEL_PAUSED'
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
                   ' in the last 14 days, and at least one campaign is still switched on. ' ||
                   'Money is going out and the data is not coming in.'
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

channel_paused as (
    select
        'CHANNEL_PAUSED'::varchar as check_name,
        'info'::varchar as severity,
        'all'::varchar as product,
        channel::varchar as entity,
        data_behind::bigint as metric_value,
        (channel || ' is ' || data_behind::varchar || ' days behind (last date ' ||
         last_data::date::varchar || '), but every campaign on it is paused' ||
         coalesce(' (last spend ' || last_spend::date::varchar || ')', '') ||
         '. The $' || round(spend_14d)::varchar || ' of spend in the last 14 days is from ' ||
         'before the pause. Data stopped because spend stopped -- expected, not a broken ' ||
         'pipe. It re-flags on its own if anything is switched back on.')::varchar as detail
    from verdict
    where status = 'CHANNEL_PAUSED'
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
select current_date, check_name, severity, product, entity, metric_value, detail from channel_paused
union all
select current_date, check_name, severity, product, entity, metric_value, detail from dormant_channels
union all
select current_date, check_name, severity, product, entity, metric_value, detail from stranded_orders
union all
select current_date, check_name, severity, product, entity, metric_value, detail from missing_tables
union all
select current_date, check_name, severity, product, entity, metric_value, detail from uk_open_date
