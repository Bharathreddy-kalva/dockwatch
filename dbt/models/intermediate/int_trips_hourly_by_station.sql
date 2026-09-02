-- Grain: one row per (station_id, hour). departures/arrivals computed
-- separately then full-outer-joined, since a station can have departures
-- with zero arrivals in a given hour (or vice versa) and both sides should
-- still produce a row rather than being dropped by an inner join.
with departures as (
    select
        start_station_id as station_id,
        date_trunc('hour', started_at) as hour,
        count(*) as departures
    from {{ ref('stg_trips') }}
    where start_station_id is not null
    group by 1, 2
),

arrivals as (
    select
        end_station_id as station_id,
        date_trunc('hour', ended_at) as hour,
        count(*) as arrivals
    from {{ ref('stg_trips') }}
    where end_station_id is not null
    group by 1, 2
)

select
    coalesce(departures.station_id, arrivals.station_id) as station_id,
    coalesce(departures.hour, arrivals.hour) as hour,
    coalesce(departures.departures, 0) as departures,
    coalesce(arrivals.arrivals, 0) as arrivals
from departures
full outer join arrivals
    on departures.station_id = arrivals.station_id
    and departures.hour = arrivals.hour
