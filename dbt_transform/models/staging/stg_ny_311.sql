{{
    config(
        materialized='table',
        partition_by={
          "field": "created_date",
          "data_type": "timestamp",
          "granularity": "day"
        },
        cluster_by=["problem", "borough"]
    )
}}

with source_data as (
    select * from {{ source('external_source', 'ny_311_external') }}
),

renamed as (
    select
        -- Identifiers
        cast(unique_key as string) as ticket_id,

        -- Timestamps
        cast(created_date as timestamp) as created_date,
        cast(closed_date as timestamp) as closed_date,
        cast(due_date as timestamp) as due_date,
        cast(resolution_action_updated_date as timestamp) as resolution_action_updated_date,
        
        -- Agencies
        agency,
        agency_name,
        
        -- Categorization
        cast(complaint_type as string) as problem,
        cast(descriptor as string) as problem_detail,
        cast(descriptor_2 as string) as additional_details,
        
        -- Location Details
        cast(location_type as string) as location_type,
        cast(incident_zip as string) as incident_zip,
        cast(incident_address as string) as incident_address,
        cast(street_name as string) as street_name,
        cast(cross_street_1 as string) as cross_street_1,
        cast(cross_street_2 as string) as cross_street_2,
        cast(intersection_street_1 as string) as intersection_street_1,
        cast(intersection_street_2 as string) as intersection_street_2,
        cast(address_type as string) as address_type,
        cast(city as string) as city,
        cast(landmark as string) as landmark,
        cast(facility_type as string) as facility_type,
        
        -- Status and Resolution
        cast(status as string) as status,
        cast(resolution_description as string) as resolution_description,
        
        -- Administrative Boundaries
        cast(community_board as string) as community_board,
        cast(council_district as integer) as council_district,
        cast(police_precinct as string) as police_precinct,
        cast(bbl as string) as bbl,
        cast(borough as string) as borough,
        
        -- Spatial Coordinates
        cast(x_coordinate_state_plane as numeric) as x_coordinate_state_plane,
        cast(y_coordinate_state_plane as numeric) as y_coordinate_state_plane,
        cast(latitude as numeric) as latitude,
        cast(longitude as numeric) as longitude,
        safe.st_geogfromtext(location) as location,
        
        -- Metadata
        cast(open_data_channel_type as string) as open_data_channel_type,
        cast(park_facility_name as string) as park_facility_name,
        cast(park_borough as string) as park_borough,
        cast(vehicle_type as string) as vehicle_type,
        cast(taxi_company_borough as string) as taxi_company_borough,
        cast(taxi_pick_up_location as string) as taxi_pick_up_location,
        cast(bridge_highway_name as string) as bridge_highway_name,
        cast(bridge_highway_direction as string) as bridge_highway_direction,
        cast(road_ramp as string) as road_ramp,
        cast(bridge_highway_segment as string) as bridge_highway_segment

    from source_data
)

select * from renamed

