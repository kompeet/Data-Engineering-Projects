{{
  config(
    materialized = "incremental",
    unique_key = '_pk'
  )
}}

with stackoverflow_questions as (

    select
        id,
        tags,
        organization_name
    from {{ ref('int_stackoverflow_questions') }}

),

stackoverflow_answers as (

    select

        _pk,
        id,
        title,
        body,
        accepted_answer_id,
        answer_count,
        comment_count,
        community_owned_datetime_utc,
        creation_datetime_utc,
        favorite_count,
        last_activity_datetime_utc,
        last_edit_datetime_utc,
        last_editor_display_name,
        last_editor_user_id,
        owner_display_name,
        owner_user_id,
        parent_id,
        post_type_id,
        score,
        tags,
        view_count

    from {{ ref('stg_stackoverflow_posts_answers') }}

    {% if is_incremental() %}

      where creation_datetime_utc > (select max(creation_datetime_utc) from {{ this }})

    {% endif %}

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
        q.tags,
        a.view_count,
        b.organization_name

    from stackoverflow_answers a
    inner join stackoverflow_questions b
    on a.parent_id = b.id

)

select * from final