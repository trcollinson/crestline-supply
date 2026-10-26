-- Raw tables in Athena: one external table per entity the upload lands in
-- the raw bucket (`crestline sim backfill --upload`), in the Glue database
-- crestline_raw. The files stay exactly as the systems wrote them; these
-- tables only describe how to read them. Dropping a table leaves its files.
--
-- Each table is partitioned by ingest_date, the day an extract was taken
-- (s3://<raw bucket>/source=<system>/entity=<name>/ingest_date=YYYY-MM-DD/).
-- MSCK REPAIR TABLE finds the partitions already in the bucket, so run this
-- file again after uploading a new extract. `crestline athena setup` does,
-- filling in ${raw_bucket} from the FoundationStack outputs.
--
-- Text timestamps stay text here; the views in sources.sql type what the
-- queries use.


-- ERP: the backfill's one-time JSONL export. The queries read the ERP from
-- the curated tables now (sources.sql); these stay for comparing the two.
-- The poller's raw pages (source=erp-api/) are read through raw_erp_api,
-- which ErpIngestStack defines. The JSON reader matches keys to columns
-- without regard to case (Athena lowercases column names).

drop table if exists raw_erp_work_centers;
create external table raw_erp_work_centers (
    Company       string,
    WorkCenter    string,
    LaborRate     double,
    BurdenRate    double
)
partitioned by (ingest_date string)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://${raw_bucket}/source=erp/entity=WorkCenters/';

drop table if exists raw_erp_customers;
create external table raw_erp_customers (
    Company       string,
    CustomerCode  string,
    Name          string
)
partitioned by (ingest_date string)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://${raw_bucket}/source=erp/entity=Customers/';

drop table if exists raw_erp_parts;
create external table raw_erp_parts (
    Company      string,
    PartNum      string,
    Description  string,
    PartType     string,
    Family       string,
    UOM          string,
    UnitPrice    double,
    UnitCost     double
)
partitioned by (ingest_date string)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://${raw_bucket}/source=erp/entity=Parts/';

drop table if exists raw_erp_part_revisions;
create external table raw_erp_part_revisions (
    Company         string,
    PartNum         string,
    RevisionNum     string,
    Approved        boolean,
    MtlPartNum      string,
    MtlQtyPerPiece  double
)
partitioned by (ingest_date string)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://${raw_bucket}/source=erp/entity=PartRevisions/';

drop table if exists raw_erp_sales_orders;
create external table raw_erp_sales_orders (
    Company       string,
    OrderNum      string,
    CustomerCode  string,
    OrderDate     string,
    LastModified  string
)
partitioned by (ingest_date string)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://${raw_bucket}/source=erp/entity=SalesOrders/';

drop table if exists raw_erp_sales_order_lines;
create external table raw_erp_sales_order_lines (
    Company       string,
    OrderNum      string,
    OrderLine     bigint,
    PartNum       string,
    RevisionNum   string,
    OrderQty      bigint,
    UnitPrice     double,
    Deleted       boolean,
    LastModified  string
)
partitioned by (ingest_date string)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://${raw_bucket}/source=erp/entity=SalesOrderLines/';

drop table if exists raw_erp_jobs;
create external table raw_erp_jobs (
    Company       string,
    JobNum        string,
    OrderNum      string,
    OrderLine     bigint,
    PartNum       string,
    RevisionNum   string,
    ProdQty       bigint,
    ReleaseDate   string,
    ReqDueDate    string,
    JobReleased   boolean,
    JobComplete   boolean,
    CompletedQty  bigint,
    LastModified  string
)
partitioned by (ingest_date string)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://${raw_bucket}/source=erp/entity=Jobs/';

drop table if exists raw_erp_job_operations;
create external table raw_erp_job_operations (
    Company              string,
    JobNum               string,
    OprSeq               bigint,
    WorkCenter           string,
    StdSetupHours        double,
    StdRunHoursPerPiece  double,
    QtyCompleted         bigint,
    ScrapQty             bigint,
    OpComplete           boolean,
    LastModified         string
)
partitioned by (ingest_date string)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://${raw_bucket}/source=erp/entity=JobOperations/';

drop table if exists raw_erp_job_materials;
create external table raw_erp_job_materials (
    Company         string,
    JobNum          string,
    MtlSeq          bigint,
    MtlPartNum      string,
    RequiredQty     double,
    IssuedQty       double,
    UnitCost        double,
    IssuedComplete  boolean,
    LastModified    string
)
partitioned by (ingest_date string)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://${raw_bucket}/source=erp/entity=JobMaterials/';

drop table if exists raw_erp_job_costs;
create external table raw_erp_job_costs (
    Company          string,
    JobNum           string,
    StdLaborCost     double,
    StdBurdenCost    double,
    StdMaterialCost  double,
    ActLaborCost     double,
    ActBurdenCost    double,
    ActMaterialCost  double,
    LastModified     string
)
partitioned by (ingest_date string)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://${raw_bucket}/source=erp/entity=JobCosts/';

drop table if exists raw_erp_labor_details;
create external table raw_erp_labor_details (
    Company       string,
    LaborDtlSeq   bigint,
    EmployeeNum   string,
    JobNum        string,
    OprSeq        bigint,
    WorkCenter    string,
    PayrollDate   string,
    LaborHrs      double,
    LaborQty      bigint,
    ScrapQty      bigint,
    LastModified  string
)
partitioned by (ingest_date string)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://${raw_bucket}/source=erp/entity=LaborDetails/';


-- MES: the nightly labor files, comma-separated with a header row.
-- Scan times are plant local time with no offset, exactly as the MES
-- writes them.

drop table if exists raw_mes_labor;
create external table raw_mes_labor (
    record_id      bigint,
    record_type    string,
    employee_id    string,
    employee_name  string,
    job_num        string,
    op_seq         int,
    work_center    string,
    scan_on        timestamp,
    scan_off       timestamp,
    qty_good       int,
    qty_scrap      int,
    notes          string
)
partitioned by (ingest_date string)
row format delimited fields terminated by ','
location 's3://${raw_bucket}/source=mes/entity=labor/'
tblproperties ('skip.header.line.count' = '1');


-- Brake gateways: one MQTT message per line.

-- PB-01's payload keys are controller register addresses (N7:12, B3:0/4)
-- that can't be column names. The JSON reader can hand a nested object back
-- as text, but it lowercases every key on the way (n7:12), and JSON paths
-- are case-sensitive: the fields silently come back null. So each message
-- is read as one line of text, exactly as published (\001 never appears in
-- it), and sources.sql picks the fields out with Athena's JSON functions.
drop table if exists raw_pb01;
create external table raw_pb01 (
    message       string
)
partitioned by (ingest_date string)
row format delimited fields terminated by '\001'
location 's3://${raw_bucket}/source=machines/entity=pb01/';

drop table if exists raw_pb02;
create external table raw_pb02 (
    topic         string,
    published_at  string,
    payload       struct<dev: string, cnt: int, gw_ts: string>
)
partitioned by (ingest_date string)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://${raw_bucket}/source=machines/entity=pb02/';


-- Register the partitions (extracts) already in the bucket.
msck repair table raw_erp_work_centers;
msck repair table raw_erp_customers;
msck repair table raw_erp_parts;
msck repair table raw_erp_part_revisions;
msck repair table raw_erp_sales_orders;
msck repair table raw_erp_sales_order_lines;
msck repair table raw_erp_jobs;
msck repair table raw_erp_job_operations;
msck repair table raw_erp_job_materials;
msck repair table raw_erp_job_costs;
msck repair table raw_erp_labor_details;
msck repair table raw_mes_labor;
msck repair table raw_pb01;
msck repair table raw_pb02;
