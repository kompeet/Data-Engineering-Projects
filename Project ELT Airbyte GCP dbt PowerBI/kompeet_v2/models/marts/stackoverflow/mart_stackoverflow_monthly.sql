with datespine as (

    {{ dbt_utils.date_spine
    (
    datepart = "month",
    start_date = "cast('2022-01-01' as date)",
    end_date = "cast('2022-12-31' as date)"
    )
    }}

),

datespine_join as (

    select distinct

        d.date_month,
        c.organization_name,

    from datespine d
        cross join {{ ref('int_googlesheet') }} c
    where c.valid_to_datetime_utc is null
    and c.tags is not null

),

company_details as (

    select distinct

        organization_name,
        tags

    from {{ ref('int_googlesheet')}}
        where valid_to_datetime_utc is null
        and tags is not null

),

stackoverflow as (

    select

        timestamp_trunc(s.creation_datetime_utc, month) as first_day_of_period,
        extract(month from s.creation_datetime_utc) as month,
        extract(quarter from s.creation_datetime_utc) as quarter,
        extract(year from s.creation_datetime_utc) as year,
        s.organization_name,
        count(distinct s.id) as post_count,
        sum(answer_count) as answer_count,
        round(avg(answer_count), 3) as avg_answer_count,
        sum(comment_count) as comment_count,
        round(avg(comment_count), 3) as avg_comment_count,
        sum(ifnull(favorite_count, 0)) as favorite_count,
        round(avg(ifnull(favorite_count, 0)), 3) as avg_favorite_count,
        sum(view_count) as view_count,
        round(avg(view_count), 3) as avg_view_count,
        round(sum(case when s.accepted_answer_id is not null then 1 else 0 end) / count(distinct s.id), 3) as accepted_answer_count,
        sum(case when s.answer_count = 0 or s.answer_count is null then 1 else 0 end) as no_answer_count,
        round(sum(case when s.answer_count = 0 or s.answer_count is null then 1 else 0 end) / count(distinct s.id), 3) as avg_no_answer_count,
        round(avg(s.score), 3) as score,
        sum(array_length((select array_agg(columns) from
            (
            select * from unnest(split(s.tags, '|'))
                except distinct
            select * from unnest(split(c.tags, ', '))
            ) as columns))) as tags_count,
        max(s.last_activity_datetime_utc) as last_activity_datetime_utc,
        max(s.last_edit_datetime_utc) as last_edit_datetime_utc

    from {{ ref('int_stackoverflow_questions')}} s
        inner join company_details c
            on s.organization_name = c.organization_name
    {{ dbt_utils.group_by(n=5) }}

),

final as (

    select

        {{ dbt_utils.generate_surrogate_key(['d.date_month', 'd.organization_name']) }} as _pk,
        d.date_month as first_day_of_period,
        extract(month from d.date_month) as month,
        extract(quarter from d.date_month) as quarter,
        extract(year from d.date_month) as year,
        d.organization_name,
        ifnull(s.post_count, 0) as post_count,
        ifnull(s.answer_count, 0) as answer_count,
        ifnull(s.avg_answer_count, 0) as avg_answer_count,
        ifnull(s.comment_count, 0) as comment_count,
        ifnull(s.avg_comment_count, 0) as avg_comment_count,
        ifnull(s.favorite_count, 0) as favorite_count,
        ifnull(s.avg_favorite_count, 0) as avg_favorite_count,
        ifnull(s.view_count, 0) as view_count,
        ifnull(s.avg_view_count, 0) as avg_view_count,
        ifnull(s.accepted_answer_count, 0) as accepted_answer_count,
        ifnull(s.no_answer_count, 0) as no_answer_count,
        ifnull(s.avg_no_answer_count, 0) as avg_no_answer_count,
        ifnull(s.score, 0) as score,
        ifnull(s.tags_count, 0) as tags_count,
        s.last_activity_datetime_utc,
        s.last_edit_datetime_utc

    from stackoverflow s
        right join datespine_join d
            on d.date_month = extract(datetime from s.first_day_of_period)
            and d.organization_name = s.organization_name

)

select * from final