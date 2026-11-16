-- job_status: one job as the ERP has it, one row per operation.
-- Parameter: job_num.
select j.JobNum                                               as job_num,
       j.PartNum                                              as part_num,
       j.RevisionNum                                          as revision,
       j.ProdQty                                              as prod_qty,
       j.CompletedQty                                         as completed_qty,
       cast(j.ReleaseDate as varchar)                         as release_date,
       cast(j.ReqDueDate as varchar)                          as due_date,
       j.JobReleased                                          as released,
       j.JobComplete                                          as complete,
       c.StdLaborCost + c.StdBurdenCost + c.StdMaterialCost   as standard_cost,
       c.ActLaborCost + c.ActBurdenCost + c.ActMaterialCost   as erp_actual_cost,
       o.OprSeq                                               as op_seq,
       o.WorkCenter                                           as work_center,
       o.QtyCompleted                                         as op_qty_completed,
       o.ScrapQty                                             as op_scrap_qty,
       o.OpComplete                                           as op_complete
from erp_jobs as j
left join erp_job_costs as c on c.JobNum = j.JobNum
left join erp_job_operations as o on o.JobNum = j.JobNum
where j.JobNum = ?
order by o.OprSeq
limit 201
