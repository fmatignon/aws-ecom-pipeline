



with source as (
    select *
    from "AwsDataCatalog"."ecom_bronze"."customers"
    
),

deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by customer_id
                order by _ingestion_ts desc
            ) as _dedup_row_num
        from source
    ) ranked
    where _dedup_row_num = 1
),

cleaned as (
    select
        -- Primary key
        customer_id,

        -- Customer identity
        first_name,
        last_name,
        lower(trim(email)) as email,
        phone,

        -- Location
        country,
        city,
        state,
        postal_code,
        address,

        -- Dates
        signup_date,
        created_at,
        updated_at,

        -- Segmentation
        customer_segment,

        -- Demographics
        date_of_birth,
        gender,

        -- QA Flags: is_valid_email (simple regex check for @ and .)
        case
            when email is not null
                 and regexp_like(lower(trim(email)), '^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$')
            then true
            else false
        end as is_valid_email,

        -- QA Flags: is_adult (date_of_birth indicates age >= 18)
        case
            when date_of_birth is not null
                 and date_diff('year', date_of_birth, current_date) >= 18
            then true
            when date_of_birth is null
            then null
            else false
        end as is_adult,

        -- QA Flags: has_contact (either email or phone is present)
        case
            when (email is not null and trim(email) != '')
                 or (phone is not null and trim(phone) != '')
            then true
            else false
        end as has_contact,

        -- Audit columns
        _ingestion_ts,
        _source_system,
        _record_hash,

        -- Partition column must be last
        ingestion_date

    from deduplicated
)

select * from cleaned