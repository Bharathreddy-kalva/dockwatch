with source as (
    select * from {{ source('dockwatch', 'trips') }}
)

select
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    extract(epoch from (ended_at - started_at))::int as duration_seconds,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual
from source
