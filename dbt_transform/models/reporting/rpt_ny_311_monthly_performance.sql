{{
    config(
        materialized='view'
    )
}}

with enriched as (
    select * from {{ ref('int_ny_311') }}
),

monthly_stats as (
    select
        date_trunc(date(created_date), month) as report_month,
        borough,
        agency,
        problem,
        is_public_view,
        count(ticket_id) as total_requests,
        count(case when status = 'Closed' then 1 end) as closed_requests,
        -- Percentage of requests closed within the month
        safe_divide(count(case when status = 'Closed' then 1 end), count(ticket_id)) * 100 as closure_rate_pct,
        avg(timestamp_diff(closed_date, created_date, hour)) as avg_resolution_hours
    from enriched
    group by 1, 2, 3, 4, 5
)

select * from monthly_stats