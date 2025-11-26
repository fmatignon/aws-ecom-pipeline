



with customer_360 as (
    select *
    from "AwsDataCatalog"."ecom_silver_ecom_gold"."customer_360"
    where total_orders > 0
),

-- Calculate RFM metrics
rfm_base as (
    select
        customer_id,
        days_since_last_order as recency_days,
        total_orders as frequency,
        total_revenue as monetary
    from customer_360
),

-- Calculate quintiles for each metric
-- Note: For recency, lower is better (more recent), so we invert the scoring
rfm_scored as (
    select
        customer_id,
        recency_days,
        frequency,
        monetary,

        -- R Score: 5 = most recent (lowest days), 1 = least recent (highest days)
        6 - ntile(5) over (order by recency_days asc nulls last) as r_score,

        -- F Score: 5 = highest frequency, 1 = lowest frequency
        ntile(5) over (order by frequency asc nulls last) as f_score,

        -- M Score: 5 = highest monetary, 1 = lowest monetary
        ntile(5) over (order by monetary asc nulls last) as m_score

    from rfm_base
    where recency_days is not null
),

rfm_final as (
    select
        customer_id,
        recency_days,
        frequency,
        monetary,
        r_score,
        f_score,
        m_score,

        -- RFM Score String (concatenated)
        cast(r_score as varchar) || cast(f_score as varchar) || cast(m_score as varchar) as rfm_score_string,

        -- RFM Score Numeric (weighted)
        (r_score * 100) + (f_score * 10) + m_score as rfm_score_numeric

    from rfm_scored
)

select * from rfm_final