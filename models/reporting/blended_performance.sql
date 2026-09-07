{{ config (
    alias = target.database + '_blended_performance'
)}}
{#
    MARKET DIMENSION (added 2026-09-07 for the UK launch)

    `market` is the general mechanism for splitting US from UK: 'US' | 'UK' | 'other' |
    'unknown'. Orders get it from their shipping country (macros/order_market.sql); paid
    rows get it from the campaign name (macros/campaign_market.sql).

    Only ONE new `channel` value is introduced -- 'Meta UK' -- because the UK deck has a
    per-channel Meta slide and that was the agreed shape. Every other channel keeps its
    existing value and is split by `market` instead. That is deliberate: each new channel
    string silently changes what the existing card predicates match, and `market` solves
    the same problem without touching them.

    !! DEPLOYING THIS REQUIRES THE CARD EDITS IN THE SAME RELEASE !!
    Cards 56129 / 56130 / 56599 contain `channel ~* 'Meta'` in their "Meta with Traffic"
    branch. 'Meta UK' matches that regex, so UK spend starts landing in a US row that
    looks untouched. They also have `channel NOT IN ('GA4','Shopify','Meta with Traffic')`,
    which will newly emit a 'Meta UK' row on the US slide. Both are fixed by adding
    `market = 'US'`; neither can be fixed before this model ships.

    TWO CLOCKS: US days are cut on America/New_York, UK days on Europe/London. `created_at`
    in the reporting layer is ALREADY US/Eastern (the time_zone var puts it there), so the
    UK day is a conversion off Eastern, not off UTC. Consequence to state on any slide that
    shows both: for a single calendar day US + UK will not sum to the storefront total --
    about 4-5% of UK orders sit on a different day under each clock, converging to nil over
    a full month.
#}
{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
WITH initial_sho_data AS (
    {% for granularity in date_granularity_list %}
    SELECT 
        '{{granularity}}' as date_granularity,
        market,
        date_trunc('{{granularity}}', effective_date) as date,
        COALESCE(SUM(gross_revenue),0) as shopify_gross_sales,
        COUNT(DISTINCT order_id) as shopify_orders,
        COUNT(DISTINCT CASE WHEN customer_order_index = 1 THEN order_id END) as shopify_first_orders
    FROM (
        SELECT 
            *,
            {{ order_market('shipping_address_country_code', 'source_name') }} as market,
            CASE 
                WHEN date_trunc('month', date) = date_trunc('month', current_date) 
                    -- each market's day on its own clock; created_at is already US/Eastern
                    THEN CASE
                        WHEN upper(trim(shipping_address_country_code)) IN ('GB','UK')
                            THEN convert_timezone('America/New_York','Europe/London', created_at)::date
                        ELSE date
                    END
                ELSE fulfillment_date
            END as effective_date
        FROM {{ ref('shopify_daily_sales_by_order') }}
        WHERE cancelled_at IS NULL
            AND subtotal_revenue > 0
            AND financial_status IN ('paid','partially_refunded','refunded','partially_paid')
    ) filtered_sales
    GROUP BY date_granularity, market, date_trunc('{{granularity}}', effective_date)
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
    ),
    
    paid_data as
    (SELECT channel, campaign_name, market, date::date, date_granularity, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, 
        COALESCE(SUM(paid_purchases),0) as paid_purchases, COALESCE(SUM(paid_revenue),0) as paid_revenue, 0 as shopify_total_sales, 0 as shopify_orders,
    0 as shopify_first_orders, 0 as shopify_subtotal_sales_adj, 0 as shopify_net_sales, 0 as shopify_gross_sales,
    0 as ga4_sessions, 0 as ga4_sessions_adjusted
    FROM
        (SELECT case when campaign_name ilike '%uk%' then 'Meta UK' else 'Meta' end as channel,
            campaign_name, {{ campaign_market('campaign_name') }} as market, date, date_granularity, 
            spend, link_clicks as clicks, impressions, 
            coalesce(purchases,0)+coalesce(purchases_shared_items,0) as paid_purchases, 
            coalesce(revenue,0)+coalesce(revenue_shared_items,0) as paid_revenue
        FROM {{ source('reporting','facebook_campaign_performance') }}
        WHERE account = 'DTC' and campaign_name !~* 'traffic'
        UNION ALL
        SELECT 'Meta with Traffic' as channel, campaign_name, {{ campaign_market('campaign_name') }} as market, date, date_granularity, 
            spend, link_clicks as clicks, impressions, 
            coalesce(purchases,0)+coalesce(purchases_shared_items,0) as paid_purchases, 
            coalesce(revenue,0)+coalesce(revenue_shared_items,0) as paid_revenue
        FROM {{ source('reporting','facebook_campaign_performance') }}
        WHERE account = 'DTC' and campaign_name ~* 'traffic'
        UNION ALL
        SELECT 'Meta Sephora' as channel, campaign_name, {{ campaign_market('campaign_name') }} as market, date, date_granularity, 
            spend, link_clicks as clicks, impressions, 
            coalesce(purchases,0)+coalesce(purchases_shared_items,0) as paid_purchases, 
            coalesce(revenue,0)+coalesce(revenue_shared_items,0) as paid_revenue
        FROM {{ source('reporting','facebook_campaign_performance') }}
        WHERE account = 'Sephora'
        UNION ALL
        SELECT 'Google Ads' as channel, campaign_name, {{ campaign_market('campaign_name') }} as market, date, date_granularity,
            spend, clicks, impressions, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
        UNION ALL
        SELECT 'Pinterest' as channel, campaign_name, {{ campaign_market('campaign_name') }} as market, date, date_granularity,
            spend, clicks, impressions, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','pinterest_ad_group_performance') }}
        UNION ALL
        SELECT 'Tiktok' as channel, campaign_name, {{ campaign_market('campaign_name') }} as market, date, date_granularity,
            spend, clicks, impressions, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','tiktok_ad_performance') }}
        WHERE campaign_id != 1861822514294002
        UNION ALL
        SELECT 'Tiktok Sephora' as channel, campaign_name, {{ campaign_market('campaign_name') }} as market, date, date_granularity,
            spend, clicks, impressions, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','tiktok_ad_performance') }}
        WHERE campaign_id = 1861822514294002
        )
    GROUP BY channel, campaign_name, market, date, date_granularity),
sho_data as
    (SELECT
            'Shopify' as channel,
            NULL as campaign_name,
            market,
            date,
            date_granularity,
            0 as spend,
            0 as clicks,
            0 as impressions,
            0 as paid_purchases,
            0 as paid_revenue, 
            0 as shopify_total_sales, 
            COALESCE(SUM(shopify_orders),0) as shopify_orders, 
            COALESCE(SUM(shopify_first_orders),0) as shopify_first_orders, 
            0 as shopify_subtotal_sales_adj,
            0 as shopify_net_sales,
            COALESCE(SUM(shopify_gross_sales),0) as shopify_gross_sales,
            0 as ga4_sessions,
            0 as ga4_sessions_adjusted
        FROM initial_sho_data 
        GROUP BY channel, campaign_name, market, date, date_granularity
    ),
ga4_data AS (
        SELECT
            'GA4' as channel,
            campaign_name,
            {{ campaign_market('campaign_name') }} as market,
            date,
            date_granularity,
            0 as spend,
            0 as clicks,
            0 as impressions,
            0 as paid_purchases,
            0 as paid_revenue,
            0 as shopify_total_sales,
            0 as shopify_orders,
            0 as shopify_first_orders,
            0 as shopify_subtotal_sales_adj,
            0 as shopify_net_sales,
            0 as shopify_gross_sales,
            COALESCE(SUM(sessions), 0) as ga4_sessions,
            -- adjustement needed to better match shopify number that we can't directly pull 
            0.8*COALESCE(SUM(sessions)) as ga4_sessions_adjusted
        FROM {{ source('reporting','ga4_performance_by_campaign') }}
        GROUP BY channel, campaign_name, market, date, date_granularity
    )
    
SELECT channel,
    campaign_name,
    market,
    date,
    date_granularity,
    spend,
    clicks,
    impressions,
    paid_purchases,
    paid_revenue,
    shopify_total_sales,
    shopify_orders,
    shopify_first_orders,
    shopify_subtotal_sales_adj,
    shopify_net_sales,
    shopify_gross_sales,
    ga4_sessions,
    ga4_sessions_adjusted
FROM (
    SELECT * FROM paid_data
    UNION ALL SELECT * FROM sho_data
    UNION ALL SELECT * FROM ga4_data
)
