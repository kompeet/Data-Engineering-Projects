{{
  config(
    materialized = "incremental",
    unique_key = '_pk'
  )
}}

with github as (

    select

        _pk,
        split(repo_name, '/')[safe_offset(0)] as repository_account,
        split(repo_name, '/')[safe_offset(1)] as repository_name,
        actor_id as user_id,
        id as event_id,
        type,
        created_at_datetime_utc

    from {{ ref('stg_github') }}

    {% if is_incremental() %}

      where created_at_datetime_utc > (select max(created_at_datetime_utc) from {{ this }} )

    {% endif %}

),

googlesheet as (

    select

        organization_name,
        repository_account,
        repository_name

    from {{ ref('int_googlesheet') }}
    where repository_account is not null and valid_to_datetime_utc is null

),

final as (

    select

        a._pk,
        a.repository_account,
        a.repository_name,
        a.user_id,
        a.event_id,
        a.type,
        a.organization_name,
        b.created_at_datetime_utc

    from github a
    inner join googlesheet b
    on a.repository_account = b.repository_account and a.repository_name = coalesce(b.repository_name, a.repository_name)

)

select * from final