with

source_answers as (
    select *
    from {{ source('stackoverflow','posts_answers') }}
),

staged as (

    select id as _pk,
    id,
    title,
    body,
    accepted_answer_id,
    answer_count,
    comment_count,
    community_owned_date as community_owned_datetime_utc,
    creation_date as creation_datetime_utc,
    favorite_count,
    last_activity_date as last_activity_datetime_utc,
    last_edit_date as last_edit_datetime_utc,
    last_editor_display_name,
    last_editor_user_id,
    owner_display_name,
    owner_user_id,
    parent_id,
    post_type_id,
    score,
    tags,
    view_count,

    from source_answers
    where EXTRACT(YEAR FROM creation_date)={{ var('year') }}
)

select *
from staged