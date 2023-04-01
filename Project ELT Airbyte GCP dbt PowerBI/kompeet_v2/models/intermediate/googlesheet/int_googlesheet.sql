with

source as (
    select *
    from {{ ref('snapshot_googlesheet') }}, UNNEST(split(tags,',')) as tags_element
),


final as (

    select

        dbt_scd_id as _pk,
        organization as organization_name,
        repository_account,
        repository_name,
        l1_type,
        l2_type,
        l3_type,
        tags,
        tags_split,
        tags_element,
        open_source_available as is_open_source_available,
        dbt_valid_from as valid_from_datetime_utc,
        dbt_valid_to as valid_to_datetime_utc,
        dbt_updated_at as updated_at_datetime_utc,
        created_at_datetime_utc

 FROM source

)

select * from final