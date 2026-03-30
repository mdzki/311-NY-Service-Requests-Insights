{{
    config(
        materialized='view'
    )
}}

with enriched as (
    select * from {{ ref('int_ny_311') }}
),

daily_stats as (
    select
        date(created_date) as report_date,
        borough,
        agency,
        problem,
        is_public_view,
        count(ticket_id) as total_requests,
        count(case when status = 'Closed' then 1 end) as closed_requests,
        -- Average resolution time in hours
        avg(timestamp_diff(closed_date, created_date, hour)) as avg_resolution_hours
    from enriched
    group by 1, 2, 3, 4, 5
)

select * from daily_stats

