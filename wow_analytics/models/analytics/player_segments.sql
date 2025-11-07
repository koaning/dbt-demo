{{ config(materialized='table') }}

with player_data as (
    select * from {{ ref('stg_players') }}
),

segments as (
    select
        player_id,
        class,
        race,
        max_level,
        days_active_pre_strike,
        num_locations_visited,
        days_since_last_activity,
        churned,

        -- Segment by engagement level
        case
            when days_active_pre_strike >= 100 and num_locations_visited >= 50 then 'High Engagement'
            when days_active_pre_strike >= 50 then 'Medium Engagement'
            else 'Low Engagement'
        end as engagement_segment,

        -- Segment by level progression
        case
            when max_level >= 80 then 'Max Level'
            when max_level >= 60 then 'Advanced'
            when max_level >= 40 then 'Intermediate'
            else 'Beginner'
        end as level_segment,

        -- Activity recency
        case
            when days_since_last_activity <= 7 then 'Active This Week'
            when days_since_last_activity <= 30 then 'Active This Month'
            when days_since_last_activity <= 90 then 'Recently Inactive'
            else 'Long Inactive'
        end as activity_segment

    from player_data
)

select * from segments
