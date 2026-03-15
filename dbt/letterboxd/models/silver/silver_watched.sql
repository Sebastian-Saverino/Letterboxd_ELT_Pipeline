{{ config(materialized='table', schema='silver') }}

with src as (
    select
        nullif(trim(list_date::text), '') as list_date_txt,
        nullif(trim(name::text), '') as name,
        nullif(trim(year::text), '') as year_txt,
        nullif(trim(letterboxd_uri::text), '') as letterboxd_uri
    from {{ source('bronze', 'watched') }}
),

typed as (
    select
        case
            when list_date_txt ~ '^\d{4}-\d{2}-\d{2}$' then list_date_txt::date
        end as list_date,
        name,
        case
            when year_txt ~ '^\d{4}$' then year_txt::int
        end as year,
        letterboxd_uri
    from src
),

deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by letterboxd_uri
                order by list_date desc nulls last
            ) as rn
        from typed
    ) t
    where rn = 1
)

select
    list_date,
    name,
    year,
    letterboxd_uri
from deduped
where letterboxd_uri is not null
