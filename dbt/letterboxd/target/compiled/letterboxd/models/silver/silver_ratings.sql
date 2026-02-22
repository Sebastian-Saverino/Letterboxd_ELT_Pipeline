

with src as (
    select
        nullif(trim(list_date), '') as list_date_txt,
        nullif(trim(name), '') as name,
        nullif(trim(year), '') as year_txt,
        nullif(trim(letterboxd_uri), '') as letterboxd_uri,
        nullif(trim(rating), '') as rating_txt
    from "letterboxd_warehouse"."bronze"."ratings"
),

typed as (
    select
        case when list_date_txt ~ '^\d{4}-\d{2}-\d{2}$' then list_date_txt::date end as list_date,
        name,
        case when year_txt ~ '^\d{4}$' then year_txt::int end as year,
        letterboxd_uri,
        case when rating_txt ~ '^\d+(\.\d+)?$' then rating_txt::numeric(3,1) end as rating
    from src
),

deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by letterboxd_uri, list_date, rating
                order by letterboxd_uri
            ) as rn
        from typed
    ) t
    where rn = 1
)

select
    list_date,
    name,
    year,
    letterboxd_uri,
    rating
from deduped