

with src as (

    select
        nullif(trim(list_date), '')       as list_date_txt,
        nullif(trim(name), '')            as name,
        nullif(trim(year), '')            as year_txt,
        nullif(trim(letterboxd_uri), '')  as letterboxd_uri

    from "letterboxd_warehouse"."bronze"."watchlist"

),

typed as (

    select
        case
            when list_date_txt ~ '^\d{4}-\d{2}-\d{2}$'
            then list_date_txt::date
        end as list_date,

        name,

        case
            when year_txt ~ '^\d{4}$'
            then year_txt::int
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
                order by list_date desc
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