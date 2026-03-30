with enriched as (
    select * from {{ ref('int_ny_311') }}
),

yearly_stats as (
    select
        extract(year from created_date) as report_year,
        borough,
        agency,
        problem,
        count(ticket_id) as total_requests,
        count(case when status = 'Closed' then 1 end) as closed_requests,
        avg(timestamp_diff(closed_date, created_date, hour)) as avg_resolution_hours
    from enriched
    group by 1, 2, 3, 4
)

select * from yearly_stats