{{ config(materialized='table') }}

-- Original wowlogs with bots filtered out
-- Joins with session stats to exclude likely bot accounts

with logs_with_sessions as (
    select * from {{ ref('stg_wowlogs_with_sessions') }}
),

bot_detection as (
    select
        player_id,
        is_likely_bot,
        max_session_duration_minutes,
        avg_session_duration_minutes,
        total_sessions,
        total_events
    from {{ ref('player_session_stats') }}
),

logs_with_bot_flag as (
    select
        logs.*,
        coalesce(bot.is_likely_bot, 0) as is_likely_bot,
        bot.max_session_duration_minutes,
        bot.avg_session_duration_minutes

    from logs_with_sessions as logs
    left join bot_detection as bot
        on logs.player_id = bot.player_id
)

-- Filter out bots and return original columns plus session info
select
    player_id,
    guild,
    level,
    race,
    class,
    "where",
    datetime,
    session_id,
    session_number,
    minutes_since_last_event,
    is_session_start

from logs_with_bot_flag
where is_likely_bot = 0
