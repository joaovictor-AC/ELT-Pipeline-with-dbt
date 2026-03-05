{% snapshot snap_repositories %}

{{
    config(
      target_schema='snapshots',
      unique_key='full_name',
      strategy='check',
      check_cols=['stargazers_count', 'forks_count', 'open_issues_count']
    )
}}

select * from {{ source('bronze', 'raw_repositories') }}

{% endsnapshot %}