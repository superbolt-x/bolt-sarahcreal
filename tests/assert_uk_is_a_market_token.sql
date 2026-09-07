-- The market of a paid campaign is derived from its name (macros/campaign_market.sql),
-- so the naming convention is load-bearing. This fails the build if 'uk' ever appears
-- inside a word rather than as a standalone market token -- e.g. a product or audience
-- name containing the letters "uk" would otherwise be silently classified as UK spend.
--
-- Passes as of 2026-09-07: the only matches are the two UK Meta campaigns.
select distinct campaign_name
from {{ ref('blended_performance') }}
where date_granularity = 'day'
  and campaign_name ilike '%uk%'
  and campaign_name !~ '(^|[^A-Za-z])[Uu][Kk]([^A-Za-z]|$)'
