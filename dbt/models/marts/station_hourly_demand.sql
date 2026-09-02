-- Grain: one row per (station_id, hour). Weather is citywide (see
-- stg_weather_hourly / CLAUDE.md's single-city scope), so every station
-- in the same hour joins to the same weather row.
--
-- indexes: table materialization means dbt drops and recreates this table
-- on every run, so the (station_id, hour) index has to be declared here to
-- survive a rebuild -- see docs/query-log.md for the query it exists for
-- and the before/after numbers.
{{ config(
    indexes=[
        {'columns': ['station_id', 'hour'], 'type': 'btree'},
    ]
) }}

with demand as (
    select * from {{ ref('int_trips_hourly_by_station') }}
),

weather as (
    select
        date_trunc('hour', observed_at) as hour,
        temperature_c,
        precipitation_mm
    from {{ ref('stg_weather_hourly') }}
)

select
    demand.station_id,
    demand.hour,
    demand.departures,
    demand.arrivals,
    demand.arrivals - demand.departures as net_flow,
    weather.temperature_c,
    weather.precipitation_mm
from demand
left join weather
    on demand.hour = weather.hour
order by demand.station_id, demand.hour
