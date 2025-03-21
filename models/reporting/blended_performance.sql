{{ config (
    alias = target.database + '_blended_performance'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}


WITH 
    sales_adj AS (
        {% for granularity in date_granularity_list %}
        SELECT 
            '{{granularity}}' as date_granularity,
            {{granularity}} as date,
            COALESCE(SUM(gross_revenue),0) - COALESCE(SUM(discount_amount),0) as subtotal_sales_adj
        FROM {{ ref('shopify_daily_sales_by_order') }}
        LEFT JOIN 
            (SELECT order_id, COALESCE(SUM(total_discounts),0) as discount_amount 
            FROM {{ source('shopify_base','shopify_orders') }} WHERE (discount_code ~* 'shopmy' OR discount_code ~* 'skeeper')
            GROUP BY order_id) USING(order_id)
        GROUP BY date_granularity, {{granularity}}
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    ),
    
    paid_data as
    (SELECT channel, date::date, date_granularity, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, 
        COALESCE(SUM(paid_purchases),0) as paid_purchases, COALESCE(SUM(paid_revenue),0) as paid_revenue, 0 as shopify_total_sales, 0 as shopify_orders,
    0 as shopify_first_orders, 0 as shopify_subtotal_sales_adj, 0 as shopify_net_sales,
    0 as ga4_sessions, 0 as ga4_sessions_adjusted
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, 
            spend, link_clicks as clicks, impressions, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','facebook_ad_performance') }}
        WHERE account = 'DTC'
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity,
            spend, clicks, impressions, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
        )
    GROUP BY channel, date, date_granularity),

sho_data as
    (
        SELECT
            'Shopify' as channel,
            date,
            date_granularity,
            0 as spend,
            0 as clicks,
            0 as impressions,
            0 as paid_purchases,
            0 as paid_revenue, 
            COALESCE(SUM(total_net_sales),0) as shopify_total_sales, 
            COALESCE(SUM(orders),0) as shopify_orders, 
            COALESCE(SUM(first_orders),0) as shopify_first_orders, 
            COALESCE(SUM(subtotal_sales_adj), 0) as shopify_subtotal_sales_adj,
            COALESCE(sum(net_sales), 0) as shopify_net_sales,
            0 as ga4_sessions,
            0 as ga4_sessions_adjusted
        FROM {{ source('reporting','shopify_sales') }} JOIN sales_adj USING (date,date_granularity)
        GROUP BY channel, date, date_granularity
    ),

ga4_data AS (
        SELECT
            'GA4' as channel,
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
            COALESCE(SUM(sessions), 0) as ga4_sessions,
            -- adjustement needed to better match shopify number that we can't directly pull 
            0.8*COALESCE(SUM(sessions)) as ga4_sessions_adjusted
        FROM {{ source('reporting','ga4_performance_by_campaign') }}
        GROUP BY channel, date, date_granularity
    )
    
SELECT channel,
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
    ga4_sessions,
    ga4_sessions_adjusted
FROM (
    SELECT * FROM paid_data
    UNION ALL SELECT * FROM sho_data
    UNION ALL SELECT * FROM ga4_data
)
