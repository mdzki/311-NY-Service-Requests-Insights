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
        created_date,
        closed_date,
        due_date,
        resolution_action_updated_date,
        
        -- Agencies
        agency,
        agency_name,
        
        -- Categorization
        complaint_type as problem,
        descriptor as problem_detail,
        descriptor_2 as additional_details,
        
        -- Location Details
        location_type,
        incident_zip,
        incident_address,
        street_name,
        cross_street_1,
        cross_street_2,
        intersection_street_1,
        intersection_street_2,
        address_type,
        city,
        landmark,
        facility_type,
        
        -- Status and Resolution
        status,
        resolution_description,
        
        -- Administrative Boundaries
        community_board,
        council_district,
        police_precinct,
        bbl,
        borough,
        
        -- Spatial Coordinates
        x_coordinate_state_plane,
        y_coordinate_state_plane,
        latitude,
        longitude,
        safe.st_geogfromtext(location) as location,
        
        -- Metadata
        open_data_channel_type,
        park_facility_name,
        park_borough,
        vehicle_type,
        taxi_company_borough,
        taxi_pick_up_location,
        bridge_highway_name,
        bridge_highway_direction,
        road_ramp,
        bridge_highway_segment

    from source_data
)

select * from renamed