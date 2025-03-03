SELECT
    CASE WHEN account_id = '816379750560368' THEN 'DTC'
         WHEN account_id = '1697010251141731' THEN 'Sephora'
    END AS account,
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
purchases,
revenue
FROM {{ ref('facebook_performance_by_campaign_age') }}
