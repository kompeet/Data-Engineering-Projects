{{
  config(
    materialized = "incremental",
    unique_key = '_pk'
  )
}}

with stackoverflow_questions as (

    select * from {{ ref('stg_stackoverflow_posts_questions') }}

    {% if is_incremental() %}

      where creation_datetime_utc > (select max(creation_datetime_utc) from {{ this }} )

    {% endif %}

),

googlesheet as (

    select

        organization as organization_name,
        unnested_tag as company_tag

    from {{ ref('int_googlesheet') }}, unnest(tags_split) as unnested_tag
        where valid_to_datetime_utc is null
        and tags is not null

),

stackoverflow_questions_tag as (

    select

        id,
        unnested_tag as stackoverflow_tag

    from stackoverflow_questions, unnest(tags_split) as unnested_tag

),

tags_match as (

    select distinct

        a.organization_name,
        b.id

    from googlesheet a
        inner join stackoverflow_questions_tag b
            on a.company_tag = b.stackoverflow_tag

),

final as (

    select

        {{ dbt_utils.generate_surrogate_key(['a._pk', 'b.organization_name']) }} as _pk,
        a.id,
        a.title,
        a.body,
        a.accepted_answer_id,
        a.answer_count,
        a.comment_count,
        a.community_owned_datetime_utc,
        a.creation_datetime_utc,
        a.favorite_count,
        a.last_activity_datetime_utc,
        a.last_edit_datetime_utc,
        a.last_editor_display_name,
        a.last_editor_user_id,
        a.owner_display_name,
        a.owner_user_id,
        a.parent_id,
        a.post_type_id,
        a.score,
        a.tags,
        a.view_count,
        b.organization_name

    from stackoverflow_questions a
    inner join tags_match b
    on a.id = b.id

)

select * from final