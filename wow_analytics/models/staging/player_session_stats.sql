{{ config(materialized='table') }}

-- Calculate session statistics per player for bot detection
-- Bots typically have extremely long sessions (playing 24/7)

with session_durations as (
    select
        player_id,
        session_id,
        session_number,
        min(datetime) as session_start,
        max(datetime) as session_end,
        count(*) as events_in_session,

        -- Calculate session duration in minutes
        datediff('minute', min(datetime), max(datetime)) as session_duration_minutes

    from {{ ref('stg_wowlogs_with_sessions') }}
    group by player_id, session_id, session_number
),

player_stats as (
    select
        player_id,

        -- Session counts
        count(*) as total_sessions,
        sum(events_in_session) as total_events,

        -- Session duration statistics (in minutes)
        round(avg(session_duration_minutes), 2) as avg_session_duration_minutes,
        round(median(session_duration_minutes), 2) as median_session_duration_minutes,
        max(session_duration_minutes) as max_session_duration_minutes,
        min(session_duration_minutes) as min_session_duration_minutes,

        -- Events per session
        round(avg(events_in_session), 2) as avg_events_per_session,
        max(events_in_session) as max_events_per_session,

        -- Bot detection heuristics
        -- Flag if max session > 12 hours (720 minutes) - likely bot
        case
            when max(session_duration_minutes) > 720 then 1
            else 0
        end as likely_bot_long_session,

        -- Flag if average session > 6 hours (360 minutes) - likely bot
        case
            when avg(session_duration_minutes) > 360 then 1
            else 0
        end as likely_bot_avg_session

    from session_durations
    group by player_id
)

select
    *,
    -- Combined bot flag (either heuristic)
    case
        when likely_bot_long_session = 1 or likely_bot_avg_session = 1 then 1
        else 0
    end as is_likely_bot

from player_stats
