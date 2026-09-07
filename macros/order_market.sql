{#
    Which DTC market an order belongs to, from its shipping country.

    Translates the classification rule Sarah Creal sent on 2026-09-07, with one amendment
    agreed the same day. Their rule had a date fallback for orders with no country, on the
    premise that shipping_address was only requested from 1 Sep 2026 onward. That premise
    does not hold for this pipeline: Fivetran lands the whole Shopify `orders` table, and
    shipping_address_country_code is populated back to 2024-06-01.

    38 orders in the entire history have no country. The 30 that survive the reporting
    filters are all point-of-sale (source_name = 'pos') -- in-person sales, which have no
    shipping address by nature. Those are US sales, so they route to 'US' rather than
    falling out of both markets, which is what the original date fallback would have done
    to any POS sale dated after the UK open date.

    'unknown' is kept deliberately. After the POS branch it should always be empty, which
    makes any non-zero 'unknown' a real new problem rather than a routine leftovers bucket.

    Note GB vs UK: Shopify sends 'GB' (306 orders all-time); 'UK' has never appeared. It is
    accepted anyway because it costs nothing. Currency is NOT a usable signal here -- every
    UK order is booked in USD -- and neither is sales channel (channel:4915287 carries both
    markets).
#}

{% macro order_market(country_code, source_name) %}
    case
        when upper(trim({{ country_code }})) = 'US'             then 'US'
        when upper(trim({{ country_code }})) in ('GB','UK')     then 'UK'
        when nullif(trim({{ country_code }}), '') is not null   then 'other'
        when {{ source_name }} = 'pos'                          then 'US'
        else 'unknown'
    end
{% endmacro %}
