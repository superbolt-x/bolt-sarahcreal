-- purchases (DTC) and purchases_shared_items (Sephora collab) are assumed to never both
-- populate on the same row -- that's what makes purchases_all = purchases + purchases_shared_items
-- safe as plain addition instead of a COALESCE-and-pick. Warn (not error) because one pre-existing
-- row already breaks the assumption: ad_id 120213817137080155 ("Now at Sephora", Sephora
-- Collaborative Ads Campaign, 2024-11-13) has purchases=1 and purchases_shared_items=2. Addition
-- is still correct there, so it's not worth blocking the build over -- but if this count grows,
-- purchases_all needs a real look. See tests/assert_no_purchase_overlap_campaign.sql for the
-- campaign-grain version.
{{ config(severity='warn') }}

select ad_id, date, purchases, purchases_shared_items
from {{ ref('facebook_ad_performance') }}
where date_granularity = 'day'
  and coalesce(purchases, 0) > 0
  and coalesce(purchases_shared_items, 0) > 0
