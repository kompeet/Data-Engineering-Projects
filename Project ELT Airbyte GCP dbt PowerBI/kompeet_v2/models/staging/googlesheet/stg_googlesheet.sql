with

source as (
    select *
    from {{ source('googlesheet','gsheet') }}
),

final as (

    select

        {{ dbt_utils.generate_surrogate_key(['organization']) }} as _pk,
        organization,
        repository_account,
        repository_name,
        l1_type,
        l2_type,
        l3_type,
        tags,
        split(tags, ',') as tags_split,
        case
            when open_source_available = 'Yes' then true
                else false
        end as is_open_source_available,
        current_datetime('UTC') as created_at_datetime_utc

    from source

)

select * from final