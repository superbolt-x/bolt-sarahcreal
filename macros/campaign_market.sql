{#
    Which DTC market a paid campaign belongs to.

    Orders carry a shipping country; spend does not. The only market signal the warehouse
    carries for a campaign is its name, so this keys off the naming convention agreed
    2026-09-07: 'UK' anywhere in the campaign name means UK, everything else is US.

    Verified at the time of writing against every campaign on Meta, Google, TikTok and
    Pinterest: all four UK campaigns carry 'UK' in the name, and there are zero false
    positives. That makes the convention load-bearing, so tests/assert_uk_is_a_market_token.sql
    fails the build if 'uk' ever appears inside a word rather than as a standalone token.

    Worth knowing: on Google Ads the authoritative signal is geo targeting
    (geoTargetConstants/2826 = United Kingdom, 2840 = United States), which Fivetran does
    not land. If the naming convention ever stops holding, that is the source to reach for.
#}

{% macro campaign_market(campaign_name) %}
    case when {{ campaign_name }} ilike '%uk%' then 'UK' else 'US' end
{% endmacro %}
