-- Campaign-grain version of tests/assert_no_purchase_overlap_ad.sql. 2 known exception rows as
-- of 2026-09-11, rolling up from the same ad-level exception (ad_id 120213817137080155, "Now at
-- Sephora", 2024-11-13). Warn (not error) for the same reason: plain addition is still correct
-- on these rows, so it's not worth blocking the build over.
{{ config(severity='warn') }}

select campaign_id, date, purchases, purchases_shared_items
from {{ ref('facebook_campaign_performance') }}
where date_granularity = 'day'
  and coalesce(purchases, 0) > 0
  and coalesce(purchases_shared_items, 0) > 0
