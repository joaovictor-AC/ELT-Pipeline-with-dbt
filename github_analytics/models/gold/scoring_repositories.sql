{{ config(materialized='table') }}

with recent_activity as (
    -- 1. Agrega métricas dos últimos 30 dias (dezembro de 2025, base >= 20251201) 
    -- e totalizadores do histórico completo para calcular as taxas da comunidade [cite: 595-596].
    select
        repo_id,
        sum(case when date_id >= 20251201 then commits_count else 0 end) as recent_commits,
        sum(case when date_id >= 20251201 then unique_committers else 0 end) as recent_contributors,
        sum(prs_opened) as total_prs,
        sum(prs_merged) as total_merged_prs,
        sum(issues_opened) as total_issues,
        sum(issues_closed) as total_closed_issues,
        avg(case when date_id >= 20251201 then avg_pr_close_hours else null end) as recent_avg_pr_close_hours,
        avg(case when date_id >= 20251201 then avg_issue_close_hours else null end) as recent_avg_issue_close_hours
    from {{ ref('fact_repo_activity') }}
    group by repo_id
),
base_metrics as (
    -- 2. Junta as dimensões descritivas com a atividade para ter as métricas em uma única CTE[cite: 597].
    select
        d.repo_id,
        d.repo_name,
        d.stars_count,
        d.forks_count,
        d.watchers_count,
        ra.recent_commits,
        ra.recent_contributors,
        ra.recent_avg_pr_close_hours,
        ra.recent_avg_issue_close_hours,
        case when ra.total_prs > 0 then (ra.total_merged_prs * 1.0 / ra.total_prs) else 0 end as merged_pr_ratio,
        case when ra.total_issues > 0 then (ra.total_closed_issues * 1.0 / ra.total_issues) else 0 end as closed_issue_ratio
    from {{ ref('dim_repository') }} d
    left join recent_activity ra on d.repo_id = ra.repo_id
),
ranked as (
    -- 3. Aplica NTILE(10) para obter um rank relativo de cada métrica[cite: 598].
    select
        repo_id,
        repo_name,
        -- Popularity (quanto maior o número, melhor o NTILE) [cite: 587]
        NTILE(10) OVER (ORDER BY stars_count ASC) as rank_stars,
        NTILE(10) OVER (ORDER BY forks_count ASC) as rank_forks,
        NTILE(10) OVER (ORDER BY watchers_count ASC) as rank_watchers,
        
        -- Activity (quanto mais atividade recente, melhor o NTILE) [cite: 587]
        NTILE(10) OVER (ORDER BY recent_commits ASC) as rank_commits,
        NTILE(10) OVER (ORDER BY recent_contributors ASC) as rank_contributors,
        
        -- Responsiveness (ATENÇÃO: para tempos de reação "menor é melhor", usamos ORDER BY DESC) [cite: 598-599]
        NTILE(10) OVER (ORDER BY COALESCE(recent_avg_pr_close_hours, 999999) DESC) as rank_pr_time,
        NTILE(10) OVER (ORDER BY COALESCE(recent_avg_issue_close_hours, 999999) DESC) as rank_issue_time,
        
        -- Community (taxas de fechamento maiores = melhor) [cite: 587]
        NTILE(10) OVER (ORDER BY merged_pr_ratio ASC) as rank_pr_ratio,
        NTILE(10) OVER (ORDER BY closed_issue_ratio ASC) as rank_issue_ratio
    from base_metrics
),
scored as (
    -- 4. Computa cada sub-score normalizando os ranks para 100 usando a fórmula: (sum_of_ranks) * 100.0 / max_possible[cite: 600].
    select
        repo_id,
        repo_name,
        -- Popularity: 3 métricas, max possible = 30
        ((rank_stars + rank_forks + rank_watchers) * 100.0 / 30.0) as score_popularity,
        -- Activity: 2 métricas, max possible = 20
        ((rank_commits + rank_contributors) * 100.0 / 20.0) as score_activity,
        -- Responsiveness: 2 métricas, max possible = 20
        ((rank_pr_time + rank_issue_time) * 100.0 / 20.0) as score_responsiveness,
        -- Community: 2 métricas, max possible = 20
        ((rank_pr_ratio + rank_issue_ratio) * 100.0 / 20.0) as score_community
    from ranked
)
-- 5. Calcula o score_global como média ponderada e adiciona o RANK() final [cite: 601-602].
select
    repo_id,
    repo_name,
    score_popularity,
    score_activity,
    score_responsiveness,
    score_community,
    (score_popularity * 0.20 + 
     score_activity * 0.30 + 
     score_responsiveness * 0.30 + 
     score_community * 0.20) as score_global,
    RANK() OVER (ORDER BY (
        score_popularity * 0.20 + 
        score_activity * 0.30 + 
        score_responsiveness * 0.30 + 
        score_community * 0.20) DESC) as ranking
from scored
order by ranking