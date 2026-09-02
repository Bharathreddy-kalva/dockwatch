with source as (
    select * from {{ source('dockwatch', 'station_status_snapshots') }}
)

select
    station_id,
    observed_at,
    num_bikes_available,
    num_bikes_disabled,
    num_docks_available,
    num_docks_disabled,
    num_ebikes_available,
    is_installed,
    is_renting,
    is_returning
from source
