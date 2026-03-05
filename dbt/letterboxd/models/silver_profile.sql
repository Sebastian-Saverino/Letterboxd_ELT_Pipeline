{{ config(materialized='table', schema='silver') }}

with src as (
    select
        nullif(trim(date_joined::text), '') as date_joined_txt,
        nullif(trim(username::text), '') as username,
        nullif(trim(given_name::text), '') as given_name,

        -- these are incorrectly typed as double precision in bronze; normalize to text
        nullif(trim(family_name::text), '') as family_name,
        nullif(trim(email_address::text), '') as email_address,
        nullif(trim(location::text), '') as location,
        nullif(trim(website::text), '') as website,

        nullif(trim(bio::text), '') as bio,
        nullif(trim(pronoun::text), '') as pronoun,
        nullif(trim(favorite_films::text), '') as favorite_films
    from {{ source('bronze', 'profile') }}
),

typed as (
    select
        case
            when date_joined_txt ~ '^\d{4}-\d{2}-\d{2}$' then date_joined_txt::date
            else null
        end as date_joined,

        username,
        given_name,
        family_name,

        -- keep as text; you can validate/clean later if needed
        email_address,
        location,
        website,

        bio,
        pronoun,
        favorite_films
    from src
),

deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by username
                order by date_joined desc nulls last
            ) as rn
        from typed
    ) t
    where rn = 1
)

select
    date_joined,
    username,
    given_name,
    family_name,
    email_address,
    location,
    website,
    bio,
    pronoun,
    favorite_films
from deduped
where username is not null;