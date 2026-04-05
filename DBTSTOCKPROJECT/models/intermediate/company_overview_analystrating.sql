SELECT
ticker,
analyst_target_price,
analyst_rating_strong_buy,
analyst_rating_buy,
analyst_rating_hold,
analyst_rating_sell,
analyst_rating_strong_sell

from {{ref('stg_companyoverview')}}

