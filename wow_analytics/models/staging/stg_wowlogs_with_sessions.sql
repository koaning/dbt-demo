{{ config(materialized='view') }}

-- Add session IDs to raw wowlogs
-- A session is defined as consecutive events by the same player within 30 minutes

with ordered_logs as (
    select
        *,
        -- Calculate time since previous event for this player
        datediff('minute',
            lag(datetime) over (partition by player_id order by datetime),
            datetime
        ) as minutes_since_last_event
    from {{ source('raw', 'wowlogs') }}
),

sessions_flagged as (
    select
        *,
        -- Flag start of new session (first event or >30 min gap)
        case
            when minutes_since_last_event is null then 1  -- First event for player
            when minutes_since_last_event > 30 then 1      -- New session after gap
            else 0
        end as is_session_start
    from ordered_logs
),

sessions_numbered as (
    select
        *,
        -- Create session ID by cumulative sum of session starts
        sum(is_session_start) over (
            partition by player_id
            order by datetime
            rows between unbounded preceding and current row
        ) as session_number
    from sessions_flagged
)

select
    player_id,
    guild,
    level,
    race,
    class,
    "where",
    datetime,

    -- Session information
    session_number,
    concat(player_id, '-', session_number) as session_id,
    minutes_since_last_event,
    is_session_start

from sessions_numbered
