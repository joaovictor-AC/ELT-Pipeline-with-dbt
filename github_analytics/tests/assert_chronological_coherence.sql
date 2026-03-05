select
    issue_number,
    repo_id,
    created_at,
    closed_at
from {{ ref('stg_issues') }}
where closed_at < created_at