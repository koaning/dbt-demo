{{ config(materialized='view') }}

-- Aggregate event-level logs into player-level features
-- Uses bot-filtered logs to ensure clean player data
with player_aggregates as (
    select
        player_id,
        -- Take the most recent class and race (in case they changed)
        max(class) as class,
        max(race) as race,

        -- Level statistics
        max(level) as max_level,
        min(level) as min_level,
        avg(level) as avg_level,

        -- Activity metrics
        count(distinct date(datetime)) as days_active,
        max(datetime) as last_activity_date,
        count(distinct "where") as num_locations_visited,

        -- Session metrics
        max(session_number) as total_sessions,
        count(*) as total_events,

        -- Guild information
        max(guild) as guild

    from {{ ref('stg_wowlogs_no_bots') }}
    group by player_id
)

select
    *,
    -- Calculate days since last activity (assuming current date for reference)
    datediff('day', last_activity_date, current_date) as days_since_last_activity,

    -- Rename for compatibility with downstream models
    days_active as days_active_pre_strike,

    -- Define churned: no activity in the last 30 days
    case
        when datediff('day', last_activity_date, current_date) > 30 then 1
        else 0
    end as churned

from player_aggregates
