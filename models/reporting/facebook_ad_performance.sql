{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

-- Campaign objective is history-tracked (facebook_raw.campaign_history / source 'campaigns'),
-- so a campaign could in principle change objective over its life. We take the latest row per
-- campaign_id, which is the intended behavior here, not an oversight.
WITH latest_campaign AS (
    SELECT campaign_id, objective
    FROM (
        SELECT
            id AS campaign_id,
            objective,
            row_number() OVER (PARTITION BY id ORDER BY updated_time DESC) AS rn
        FROM {{ source('facebook_raw', 'campaigns') }}
    ) ranked
    WHERE rn = 1
)

SELECT
    CASE WHEN account_id = '816379750560368' THEN 'DTC'
         WHEN account_id in ('1697010251141731','1683871059295011') THEN 'Sephora'
    END AS account,
fp.campaign_name,
fp.campaign_id,
fp.campaign_effective_status,
fp.campaign_type_default,
lc.objective AS campaign_objective,
fp.adset_name,
fp.adset_id,
fp.adset_effective_status,
fp.audience,
fp.ad_name,
fp.ad_id,
fp.ad_effective_status,
fp.visual,
fp.copy,
fp.format_visual,
fp.visual_copy,
fp.date,
fp.date_granularity,
fp.spend,
fp.impressions,
fp.link_clicks,
fp.add_to_cart,
fp.onsite_web_purchase as purchases,
fp.onsite_web_purchase_value as revenue,
fp.omni_purchase_with_shared_items as purchases_shared_items,
fp.omni_purchase_with_shared_items_value as revenue_shared_items,
-- Sephora collaborative-ads campaigns record conversions in the shared_items columns instead of
-- purchases/revenue; the two families never populate on the same row (see the documented
-- exception in tests/assert_no_purchase_overlap_ad.sql), so plain addition doesn't double-count.
COALESCE(fp.onsite_web_purchase, 0) + COALESCE(fp.omni_purchase_with_shared_items, 0) AS purchases_all,
COALESCE(fp.onsite_web_purchase_value, 0) + COALESCE(fp.omni_purchase_with_shared_items_value, 0) AS revenue_all,
fp.onfacebook_leads as leads
FROM {{ ref('facebook_performance_by_ad') }} fp
LEFT JOIN latest_campaign lc ON lc.campaign_id = fp.campaign_id
