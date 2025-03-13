{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT
    CASE WHEN account_id = '816379750560368' THEN 'DTC'
         WHEN account_id = '1697010251141731' THEN 'Sephora'
    END AS account,
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
onsite_web_purchase as purchases,
onsite_web_purchase_value as revenue,
omni_purchase_with_shared_items as purchases_shared_items,
omni_purchase_with_shared_items_value as revenue_shared_items
FROM {{ ref('facebook_performance_by_ad') }}
