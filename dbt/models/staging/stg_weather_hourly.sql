with source as (
    select * from {{ source('dockwatch', 'weather_hourly') }}
)

select
    observed_at,
    temperature_c,
    precipitation_mm
from source
