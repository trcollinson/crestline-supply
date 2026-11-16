-- job_margin: the margin table (queries/margin_gap.sql) for the
-- completed jobs released in the window.
-- Parameters: the window's first and last plant day.
select m.brake,
       coalesce(m.setup_shift, '')                                          as shift,
       count(*)                                                             as jobs,
       cast(round(sum(m.revenue)) as bigint)                                as revenue,
       round(100 * (1 - sum(m.standard_cost) / sum(m.revenue)), 1)          as standard_margin_pct,
       round(100 * (1 - sum(m.erp_cost)      / sum(m.revenue)), 1)          as erp_margin_pct,
       round(100 * (1 - sum(m.rebuilt_cost)  / sum(m.revenue)), 1)          as rebuilt_margin_pct,
       round(100 * (sum(m.rebuilt_cost) - sum(m.erp_cost)) / sum(m.revenue), 1) as gap_pts,
       round(sum(m.setup_hours) / sum(m.std_setup_hours), 2)                as setup_vs_standard
from job_margins as m
join erp_jobs as j on j.JobNum = m.job_num
where cast(j.ReleaseDate as date) between cast(? as date) and cast(? as date)
group by 1, 2
order by 1, 2
limit 201
