{# with staging as (
    select * from {{ ref('stg_ny_311') }}
),

all_agencies_meta as (
    -- Reference to the seed for all agencies
    select 
        agency,
        problem,
        problem_detail,
        public_view,
        location_type
    from {{ ref('ny_311_all_agencies_complaint_details') }}
),

hpd_meta as (
    -- Reference to the HPD specific seed
    select 
        problem,
        problem_detail,
        bmp as building_management_program
    from {{ ref('ny_311_hpd_complaint_details') }}
    group by 1, 2, 3 -- Ensure uniqueness for joining
),

enriched as (
    select
        s.*,
        -- Enriching with Public View status from meta
        coalesce(a.public_view, 'Unknown') as is_public_view,
        -- Enriching with HPD Building Management Program
        h.building_management_program
    from staging s
    left join all_agencies_meta a 
        on s.agency = a.agency 
        and s.problem = a.problem 
        and s.problem_detail = a.problem_detail
    left join hpd_meta h
        on s.problem = h.problem 
        and s.problem_detail = h.problem_detail
        and s.agency = 'Department of Housing Preservation and Development'
)

select 

-- Identifiers
ticket_id,	


count(ticket_id) 
    over(partition by problem, created_date, agency, incident_address, 
    x_coordinate_state_plane, y_coordinate_state_plane, 
    problem_detail, additional_details
) as multiple_tickets,

-- Timestamps
created_date,	
closed_date,	
due_date,	
resolution_action_updated_date,

-- Agencies
agency,	
agency_name,	

-- Categorization
problem,	
problem_detail,	
additional_details,	

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
location,	
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

 from enriched

 qualify row_number() over(partition by problem, created_date, agency, incident_address, 
            x_coordinate_state_plane, y_coordinate_state_plane, 
            problem_detail, additional_details
            order by closed_date desc
        ) = 1 #}


        with staging as (
    select * from {{ ref('stg_ny_311') }}
),

all_agencies_meta as (
    -- Reference to the seed for all agencies with ny_ prefix
    select 
        Agency as agency,
        `Complaint Type` as problem,
        Descriptor as problem_detail,
        `Public View` as public_view,
        Location_Type as location_type
    from {{ ref('ny_311_all_agencies_complaint_details') }}
    where Agency is not null
    group by 1, 2, 3, 4, 5
),

hpd_meta as (
    -- Reference to the HPD specific seed with ny_ prefix
    select 
        `Complaint Type` as problem,
        Descriptor as problem_detail,
        BMP as building_management_program
    from {{ ref('ny_311_hpd_complaint_details') }}
    where `Complaint Type` is not null
    group by 1, 2, 3
),

enriched as (
    select
        s.*,
        -- Enriching with Public View status from meta
        coalesce(a.public_view, 'Unknown') as is_public_view,
        -- Enriching with HPD Building Management Program
        h.building_management_program
    from staging s
    left join all_agencies_meta a 
        on s.agency = a.agency 
        and s.problem = a.problem 
        and s.problem_detail = a.problem_detail
    left join hpd_meta h
        on s.problem = h.problem 
        and s.problem_detail = h.problem_detail
        and s.agency = 'Department of Housing Preservation and Development'
),

final as (
    select 
        *,
        -- Count multiple tickets for the same issue/location/time
        count(ticket_id) over(
            partition by 
                problem, 
                created_date, 
                agency, 
                incident_address, 
                x_coordinate_state_plane, 
                y_coordinate_state_plane, 
                problem_detail, 
                additional_details
        ) as multiple_tickets_count
    from enriched
)

select * from final
-- Deduplicate: Keep the record with the latest closed_date if duplicates exist
qualify row_number() over(
    partition by 
        problem, 
        created_date, 
        agency, 
        incident_address, 
        x_coordinate_state_plane, 
        y_coordinate_state_plane, 
        problem_detail, 
        additional_details
    order by closed_date desc
) = 1