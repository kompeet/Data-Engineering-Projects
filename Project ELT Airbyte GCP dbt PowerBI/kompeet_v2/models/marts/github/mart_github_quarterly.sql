{% set sum_events = {
    "issues": "IssuesEvent",
    "watch": "WatchEvent",
    "fork": "ForkEvent",
    "push": "PushEvent",
    "pr": "PullRequestEvent",
    "delete": "DeleteEvent",
    "public": "PublicEvent",
    "create": "CreateEvent",
    "gollum": "GollumEvent",
    "member": "MemberEvent",
    "commit_comment": "CommitCommentEvent",
} %}

with datespine as (

    {{ dbt_utils.date_spine
    (
    datepart = "quarter",
    start_date = "cast('2022-01-01' as date)",
    end_date = "cast('2022-12-31' as date)"
    )
    }}

),

datespine_join as (

    select distinct

        d.date_quarter,
        c.organization_name,
        c.repository_account,
        coalesce(c.repository_name, c.repository_account) as repository_name

    from datespine d
    cross join {{ ref('int_googlesheet') }} c
    where c.repository_account is not null and c.valid_to_datetime_utc is null

),

github as (

    select

        timestamp_trunc(min(g.created_at_datetime_utc), month) as first_day_of_period,
        extract(month from min(g.created_at_datetime_utc)) as month,
        extract(quarter from g.created_at_datetime_utc) as quarter,
        extract(year from g.created_at_datetime_utc) as year,
        g.organization_name,
        g.repository_account,
        c.repository_name,
        count(g.event_id) as event_count,
        count(distinct g.user_id) as user_count,

        {% for key, value in sum_events.items()  %}

            sum(case when g.type = '{{value}}' then 1 else 0 end) as {{key}}_count

        {% if not loop.last %} , {% endif %}

        {% endfor %}

    from {{ ref('int_github') }} g
    inner join {{ ref('int_googlesheet') }} c
    on g.organization_name = c.organization_name

    group by 3, 4, 5, 6, 7

),

final as (

    select

        {{ dbt_utils.generate_surrogate_key(['d.date_quarter', 'd.organization_name']) }} as _pk,
        d.date_quarter as first_day_of_period,
        extract(month from d.date_quarter) as month,
        extract(quarter from d.date_quarter) as quarter,
        extract(year from d.date_quarter) as year,
        d.organization_name,
        d.repository_account,
        d.repository_name,
        ifnull(g.event_count, 0) as event_count,
        ifnull(g.user_count, 0) as user_count,

        {% for key, value in sum_events.items()  %}

            ifnull(g.{{key}}_count, 0) as {{key}}_count

        {% if not loop.last %} , {% endif %}

        {% endfor %},

        (
        {%- for key in sum_events %}

            ifnull(g.{{key}}_count, 0)

        {% if not loop.last -%} + {% endif %}
        {% endfor %}
        ) as total_event_count

    from github g
    right join datespine_join d
    on d.date_quarter = extract(datetime from g.first_day_of_period) and d.organization_name = g.organization_name

)

select * from final