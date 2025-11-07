{{ config(materialized='table') }}

with player_segments as (
    select * from {{ ref('player_segments') }}
),

churn_metrics as (
    select
        class,
        race,
        engagement_segment,
        level_segment,
        activity_segment,

        count(*) as total_players,
        sum(churned) as churned_players,
        round(100.0 * sum(churned) / count(*), 2) as churn_rate,

        round(avg(days_active_pre_strike), 2) as avg_days_active,
        round(avg(num_locations_visited), 2) as avg_locations_visited,
        round(avg(max_level), 2) as avg_max_level

    from player_segments
    group by class, race, engagement_segment, level_segment, activity_segment
    having count(*) >= 5  -- Only include segments with at least 5 players
)

select * from churn_metrics
order by churn_rate desc
