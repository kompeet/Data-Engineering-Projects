{% snapshot snapshot_googlesheet %}

    {{
        config(
          target_database='dbt-project-373811',
          target_schema='dbt_pkomaromi',
          strategy='check',
          unique_key='organization',
          check_cols=['organization','repository_name','repository_account','tags','l1_type','l2_type','l3_type','open_source_available'],
          invalidate_hard_deletes=True,
        )
    }}

    select *
    from {{ source('googlesheet','gsheet') }}

{% endsnapshot %}