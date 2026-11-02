# Crestline Supply — Plant Simulator & Data Platform Spec

> Working spec for the `crestline-supply` repo. This is the source of truth for design decisions.

---

## 1. Purpose

Crestline Supply is a **fictional mid-market metal fabrication company** used as a running example for manufacturing data, AWS, and AI agents.

The repo has two jobs:

1. **Simulate a realistic plant and its messy systems.** A hidden "ground truth" simulation models what actually happened on the shop floor. Three fake interfaces (ERP, MES, machines) each expose a *partial, distorted* view of that truth, the way real mid-market systems do.
2. **Build the data platform on AWS** that ingests those sources, cleans and joins them, and answers business questions (margin by line/shift, downtime, scrap attribution, traceability), eventually with an AI agent on top.

The credibility of the example depends on the mess being *realistic*, not random. Every imperfection should be something a plant IT person or integrator would recognize.

### Design principles

- **Simulate the plant, then fake the interfaces.** Never generate interface data directly; always derive it from the ground truth.
- **The answer key is hidden but measurable.** The truth is written to Parquet so we can score how well the pipelines reconstruct reality. Pipeline code must never read it.
- **Every mess is a configurable knob.** One config file (`mess.yaml`) controls all imperfections. `mess_level: 0` is a clean plant for teaching; higher levels prove the pipeline.
- **Deterministic.** Same seed + same config = identical output.
- **Runs on a laptop.** Everything works locally before it touches AWS: the backfill's files, the services run in process or with `uv run`, and DuckDB. No container or broker is needed (the MQTT path exists only in AWS, §4.3).
- **Vendor-neutral.** Interfaces are modeled on patterns common across mid-market ERPs, MES products, and PLC gateways. Never name or imitate a specific vendor's API.

---

## 2. The company

| Attribute | Value |
|---|---|
| Company | Crestline Supply (company code `CRE`) |
| Plant | Plant 10 (`P10`) |
| Timezone | `America/Denver` (DST matters, see mess catalog) |
| Products | Electrical enclosures, mounting brackets, custom panels |
| Customers | HVAC, electrical, and industrial equipment makers |
| Shifts | 1st: 06:00–14:30, 2nd: 14:30–23:00, Mon–Fri; each shift has a 10-min break, a 30-min meal, and a 10-min break; occasional Saturday OT (1st shift only) |
| Volume | ~15–25 jobs released per day, quantities 10–500 (log-normal: most jobs are a few dozen pieces, a few run into the hundreds). Jobs are released at the ordered quantity plus a 5% scrap allowance, rounded up (the planner's standing rule); overs go to stock, shorts ship short |

### 2.1 Customers (fictional)

| Code | Name |
|---|---|
| ALP | Alpine Mechanical |
| BAS | Basin HVAC Systems |
| COR | Cordova Controls |
| DRE | Delta Ridge Electric |
| EVE | Everline Equipment |
| FOX | Foxglove Industrial |
| GPR | Granite Peak Refrigeration |
| HAR | Harbor Automation |

### 2.2 Materials

| Material ID | Description | Cost/lb (USD) | Sheet (lb) |
|---|---|---|---|
| CRS-14 | Cold-rolled steel, 14 ga | 0.62 | 100 |
| CRS-16 | Cold-rolled steel, 16 ga | 0.64 | 80 |
| AL-5052-12 | Aluminum 5052, 12 ga | 2.85 | 36 |
| SS-304-16 | Stainless 304, 16 ga | 2.40 | 85 |

`Sheet (lb)` is one 4 × 8 ft sheet; the laser's `SheetCount` (§4.3) counts a new sheet each time the parts cut outgrow 70% of one.

Material arrives in **lots** (`LOT-<material>-<5 digits>`) with a supplier name and **heat number**. Each lot is ~2,000–6,000 lb. Jobs consume from the oldest open lot of the right material (FIFO), and a job may span two lots.

### 2.3 Work centers and machines

| Work center | Machine / station | Type | Interface generation |
|---|---|---|---|
| LASER | `LSR-01` fiber laser | Discrete, automated | Modern: Sparkplug B over MQTT |
| DEBURR | `DB-01` manual station | Manual | None (MES scans only) |
| BRAKE | `PB-01` press brake | Discrete | Mid-age: flat JSON via edge gateway `GW01` |
| BRAKE | `PB-02` press brake | Discrete | Legacy: polled 16-bit counter via gateway `GW02` |
| PEM | `PEM-01` hardware insertion press | Discrete | Legacy: polled 16-bit counter via gateway `GW02` |
| POWDER | `PC-LINE` powder coat line | Continuous process | Process stream via gateway `GW03` |
| ASSY | `AS-01` assembly, pack, final inspection | Manual | None (MES scans only) |

### 2.4 Rates (used by ERP standard costing)

| Work center | Machine burden $/hr | Labor $/hr |
|---|---|---|
| LASER | 145 | 38 |
| DEBURR | 0 | 32 |
| BRAKE | 85 | 38 |
| PEM | 55 | 34 |
| POWDER | 120 | 34 |
| ASSY | 0 | 32 |

### 2.5 Parts (starter set — expand to ~20)

Standard times are **setup minutes / run minutes per piece**. POWDER run time is per piece but the line runs in racks (see §3.4).

Prices are what a contract fabricator would quote: standard cost for a 25-piece lot (the median job size), i.e. material plus setup and run hours at the §2.4 rates, marked up to a 30% margin and rounded to the nearest $0.25. Smaller lots don't cover their setups and lose money even at standard; larger lots do better than 30%.

| Part | Rev | Family | Material | Weight (lb) | Price | Routing (op: work center, setup/run) |
|---|---|---|---|---|---|---|
| ENC-2010 | B | ENC | CRS-16 | 6.8 | 23.50 | 10 LASER 15/0.8 · 20 DEBURR 0/0.5 · 30 BRAKE 25/1.2 · 40 PEM 10/0.4 · 50 POWDER 0/0.3 · 60 ASSY 5/2.0 |
| ENC-2020 | A | ENC | CRS-14 | 9.4 | 29.75 | 10 LASER 15/1.1 · 20 DEBURR 0/0.6 · 30 BRAKE 30/1.6 · 40 PEM 10/0.5 · 50 POWDER 0/0.4 · 60 ASSY 5/2.5 |
| ENC-3100 | C | ENC | SS-304-16 | 7.2 | 44.50 | 10 LASER 20/1.0 · 20 DEBURR 0/0.8 · 30 BRAKE 30/1.4 · 40 PEM 10/0.5 · 60 ASSY 5/2.2 |
| ENC-4200 | A | ENC | AL-5052-12 | 4.1 | 33.00 | 10 LASER 15/0.9 · 20 DEBURR 0/0.7 · 30 BRAKE 25/1.3 · 50 POWDER 0/0.3 · 60 ASSY 5/1.8 |
| BRK-1042 | C | BRK | CRS-14 | 1.2 | 7.75 | 10 LASER 10/0.25 · 20 DEBURR 0/0.2 · 30 BRAKE 20/0.35 · 50 POWDER 0/0.1 |
| BRK-1050 | A | BRK | CRS-14 | 1.6 | 8.50 | 10 LASER 10/0.3 · 20 DEBURR 0/0.2 · 30 BRAKE 20/0.4 · 50 POWDER 0/0.1 |
| BRK-1100 | B | BRK | CRS-16 | 0.9 | 6.50 | 10 LASER 10/0.2 · 30 BRAKE 15/0.3 · 50 POWDER 0/0.1 |
| BRK-1210 | A | BRK | SS-304-16 | 1.1 | 10.50 | 10 LASER 12/0.25 · 20 DEBURR 0/0.3 · 30 BRAKE 20/0.35 |
| PNL-5001 | B | PNL | CRS-16 | 3.5 | 10.75 | 10 LASER 12/0.5 · 20 DEBURR 0/0.4 · 40 PEM 10/0.3 · 50 POWDER 0/0.2 · 60 ASSY 5/0.8 |
| PNL-5010 | A | PNL | AL-5052-12 | 2.2 | 15.50 | 10 LASER 12/0.45 · 20 DEBURR 0/0.4 · 40 PEM 10/0.3 · 60 ASSY 5/0.8 |
| PNL-5100 | D | PNL | CRS-14 | 5.8 | 20.25 | 10 LASER 15/0.7 · 20 DEBURR 0/0.5 · 30 BRAKE 25/0.9 · 40 PEM 10/0.4 · 50 POWDER 0/0.25 · 60 ASSY 5/1.0 |
| PNL-5200 | A | PNL | SS-304-16 | 4.0 | 21.75 | 10 LASER 15/0.6 · 20 DEBURR 0/0.6 · 40 PEM 10/0.3 · 60 ASSY 5/1.0 |

Each part/rev has one or more **NC programs** for the laser (e.g. `BRK1042C_14GA.nc`) and a **brake program number** (e.g. `P1042`). See §4.3 for why this matters.

Revisions change over time: at least two parts should have a revision bump mid-backfill (e.g. `BRK-1042` C → D on day 50) so old and new revisions coexist in open jobs.

M1 ships the 12-part starter set above with no revision changes. Expanding to ~20 parts, the mid-window revision bumps, and `reference/brake_program_map.csv` are deferred past M3 (decided at M3's scope lock): adding parts changes the job generator's draws, which would move every number already published from the 12-part plant. They come later, drawn from their own RNG stream, together with the laser's `ProgramName` join (§4.3), which is what needs old and new revisions in flight at the same time.

---

## 3. Ground truth simulation (`sim/`)

Discrete-event simulation using **SimPy**, driven by a seeded RNG.

### 3.1 Entities

- **Sales orders / lines**, entered 1–5 working days before release → **jobs** (`J` + 6 digits, e.g. `J104221`), one per line, created unreleased at order entry and released by the planner.
- **Job operations** follow the part routing. Each operation queues at its work center.
- **Machines** have a state machine: `IDLE`, `SETUP`, `RUNNING`, `DOWN`, `PLANNED_STOP` (breaks, shift change, no shift).
- **Operators** (~14 named fictional employees, split across shifts) are assigned to work centers; some float between the two brakes.
- **Parts produced**: every cycle produces a piece that is good or scrap, with a true cause.
- **Material consumption**: which lot fed which job, how many lb.

### 3.2 Truth downtime reasons

`JAM`, `TOOLING`, `MATERIAL_WAIT`, `MAINTENANCE`, `QUALITY_HOLD`, `NO_OPERATOR`, `PROGRAM_ERROR`, `MICROSTOP` (< 2 min).

### 3.3 Truth scrap causes

`SETUP_SCRAP`, `NOZZLE_WEAR`, `BAD_MATERIAL_FLATNESS`, `OVEN_UNDERCURE`, `OPERATOR_ERROR`, `RANDOM` (baseline ~1%).

### 3.4 Powder coat line behavior

PC-LINE is a continuous conveyor. Parts are hung on racks/hooks; the line moves at a set speed (ft/min) through four oven zones. Parts from multiple jobs can share the line at the same time. The line knows **hooks and temperatures, not parts or jobs**. Undercure defects are *not visible* at POWDER; they are found at ASSY inspection 1–2 days later.

### 3.5 Hidden scenarios

Each scenario is configurable in `scenarios.yaml` and can be toggled off. Day numbers are relative to the start of the simulation window. **Scenarios are built in the milestone that needs them** (see §10): M1 builds the scenario framework plus `pb02_second_shift_setups` only. M3 adds none (decided at its scope lock): `laser_nozzle_degradation` changes scrap, not cycle counts or downtime, so M3's evals can't use it, and it moves to M5 as a morning-brief anomaly; `oven_zone3_drift` needs the MES quality file, ASSY inspection 1–2 days after coating, and `truth_scrap`, which together are a milestone of their own (M8). `bad_material_lot` stays with M6.

| ID | Story | Mechanism |
| --- | --- | --- |
| `pb02_second_shift_setups` | The slow second shift | PB-02 setups on 2nd shift take `2.8×` standard for the whole window. ERP costs at standard, so affected jobs look profitable. |
| `laser_nozzle_degradation` | Degrading nozzle | Starting day 41, LSR-01 scrap (`NOZZLE_WEAR`) ramps from 1% to 9% over 10 days until a `MAINTENANCE` event resets it. |
| `oven_zone3_drift` | Oven runs cool | Days 58–61, PC-LINE zone 3 runs ~25 °F below its 400 °F setpoint. 15–30% of parts coated in that window fail at ASSY inspection 1–2 days later as `OVEN_UNDERCURE`. |
| `bad_material_lot` | Bad steel | One CRS-14 lot received on day 70 has flatness problems: ~12% `BAD_MATERIAL_FLATNESS` scrap at LASER and BRAKE for every job that consumes it (~8 jobs). |

### 3.6 Answer key output

Written as Parquet to `out/truth/` (never mounted into pipeline containers, never uploaded to the raw data bucket):

- `truth_jobs`, `truth_operations`
- `truth_machine_states` (interval table: machine, state, reason, start, end)
- `truth_cycles` (one row per piece: machine, job, op, timestamp, good/scrap, cause)
- `truth_scrap` (M8, with the quality file; until then every piece's scrap and cause is on `truth_cycles`)
- `truth_material_consumption` (lot → job, lb; M6, with material lots)
- `truth_labor` (operator intervals by job/op)
- `truth_costs` (true cost per job using actual times × rates)

All truth timestamps are UTC.

---

## 4. Interfaces

Simulation writes to **backing stores** (SQLite for ERP and MES, files for nightly drops, a message log for machines). Interface services read from those stores. This makes backfill and live mode share the same interface code.

### 4.1 ERP (`interfaces/erp-api/`) — REST

Modeled on OData-style mid-market ERP APIs. Entity and field names (`JobNum`, `PartNum`, `ProdQty`, `OprSeq`, `LaborHrs`, ...) follow conventions common across mid-market ERPs; the API surface (paths, auth, service layout) copies no product's.

**Auth:** `POST /erp/api/v2/auth/token`, form-encoded OAuth 2 client credentials (`grant_type=client_credentials`, `client_id`, `client_secret`) → `{"access_token", "token_type": "Bearer", "expires_in": 3600}` (`expires_in` is `token_ttl_seconds`, §5; absent if tokens never expire). Tokens are signed rather than stored, so every copy of the API with the same signing key accepts them. Every entity request needs `Authorization: Bearer <token>`; a missing, invalid, or expired token is a `401`.

**Base:** `/erp/api/v2/`

**Entities:** `WorkCenters` (labor and burden rates per work center, §2.4), `Customers`, `Parts`, `PartRevisions`, `SalesOrders`, `SalesOrderLines`, `Jobs`, `JobOperations`, `JobMaterials`, `LaborDetails`, `MaterialLots`, `JobCosts`

All but `MaterialLots` are built. The backfill writes them to `out/erp/erp.db` and the same rows, shaped as API records, to `out/erp/export/<Entity>.jsonl`; the API serves `erp.db`:

| Entity | Key | Fields besides `Company` and `LastModified` |
|---|---|---|
| `WorkCenters` | `WorkCenter` | `LaborRate`, `BurdenRate` ($/hr) |
| `Customers` | `CustomerCode` | `Name` |
| `Parts` | `PartNum` | `Description`, `PartType` (`M` manufactured, `P` purchased), `Family`, `UOM` (`EA` or `LB`), `UnitPrice` (manufactured), `UnitCost` (standard $/lb, purchased). The four sheet materials of §2.2 are purchased parts. |
| `PartRevisions` | `PartNum`, `RevisionNum` | `Approved`, `MtlPartNum` (the sheet it is cut from), `MtlQtyPerPiece` (lb) |
| `SalesOrders` | `OrderNum` | `CustomerCode`, `OrderDate` |
| `SalesOrderLines` | `OrderNum`, `OrderLine` | `PartNum`, `RevisionNum`, `OrderQty`, `UnitPrice` |
| `Jobs` | `JobNum` | `OrderNum`, `OrderLine`, `PartNum`, `RevisionNum`, `ProdQty`, `ReleaseDate`, `ReqDueDate`, `JobReleased`, `JobComplete`, `CompletedQty` |
| `JobOperations` | `JobNum`, `OprSeq` | `WorkCenter` (never the machine), `StdSetupHours`, `StdRunHoursPerPiece`, `QtyCompleted`, `ScrapQty`, `OpComplete` |
| `JobMaterials` | `JobNum`, `MtlSeq` | `MtlPartNum`, `RequiredQty` and `IssuedQty` (lb, issued at release), `UnitCost`, `IssuedComplete` |
| `LaborDetails` | `LaborDtlSeq` | `EmployeeNum`, `JobNum`, `OprSeq`, `WorkCenter`, `PayrollDate`, `LaborHrs`, `LaborQty`, `ScrapQty` (`LastModified` is when it posted) |
| `JobCosts` | `JobNum` | `StdLaborCost`, `StdBurdenCost`, `StdMaterialCost`, `ActLaborCost`, `ActBurdenCost`, `ActMaterialCost` |

Master data (`WorkCenters`, `Customers`, `Parts`, `PartRevisions`) has no `LastModified`: it was set up before the window and never changes in it, so a client reloads it in full. M3's revision bumps will add one to `PartRevisions`. `MaterialLots` arrives with material lots in M6. In `erp.db`, true/false columns are declared `BOOLEAN` (SQLite still stores 0 and 1), so the API serves `true` and `false` from the store's own schema without importing the simulator.

Clean values (every mess off): numbers are JSON numbers, dates are `YYYY-MM-DD`, timestamps are ISO 8601 UTC (`2026-10-02T15:04:11Z`), and a row's `LastModified` is the moment of the change that last touched it.

**What changes, and when.** The simulator emits the ERP's change log, derived from the order book, the release times, and the MES labor; the ERP as it stood at any instant is that log replayed up to then.

| Event | When | Rows |
|---|---|---|
| Order entry | `OrderDate`, at some second of 08:00–17:00 local | insert `SalesOrders`, `SalesOrderLines`, and the job the line creates: `Jobs` (`JobReleased` false), `JobOperations` (qty 0), `JobMaterials` (required, not issued), `JobCosts` (standard costs, actuals 0) |
| Job release | 1–5 working days later, during 1st shift | update `Jobs` (`JobReleased` true), `JobMaterials` (issued in full), `JobCosts` (`ActMaterialCost`) |
| Nightly labor batch | 22:00 local on the next working day | insert that day's `LaborDetails`; update the `JobOperations` they posted to and those jobs' `Jobs` and `JobCosts` |

Master data has no event. Rows are listed in the order the ERP created them.

**The store.** `erp.db` keeps every version of every row: `Jobs` holds the current rows with `SysValidFrom`, the moment each version began, and `Jobs_History` every superseded version with `SysValidFrom` and `SysValidTo`, the way system-versioned tables do. Master data has `SysValidFrom` NULL (in the ERP since go-live). Neither the `Sys` columns nor the `_History` tables are part of the API: a field or entity by that name is a 400 or 404. In clean mode `SysValidFrom` equals `LastModified`; the `LastModified` messes break that on purpose. The JSONL export is the current rows.

**The server clock.** With no clock configured, the API serves the store as it stands and its now is the wall clock. Given `CRESTLINE_ERP_CLOCK_START` (ISO 8601 with an offset), optional `CRESTLINE_ERP_CLOCK_SPEED` (ERP seconds per wall second; default 1; 0 freezes it) and optional `CRESTLINE_ERP_CLOCK_ANCHOR` (the wall time at which the clock read the start; the app's start unless set, and set whenever several copies of the API must agree), it replays the ERP's history: every request is served as of the clock, in whole seconds. The replay stops at the store's last change, the end of its history (the backfill's last labor batch): past it, the clock reads that instant, so a replay left running never carries a client into dates the history never had. `CRESTLINE_ERP_CLOCK_STOPS=false` lets it run on, as a live ERP's clock does after its last write; the extraction eval sets it, since tokens and slow pages count the ERP's clock. Every response's `Date` header is the ERP's time, as any server's is, and so is `X-Server-Time` (ISO 8601): a front door may replace `Date` with its own wall clock, and an AWS Lambda function URL does (it renames ours `x-amzn-Remapped-date`, and `WWW-Authenticate` likewise), so a client takes the ERP's time from `X-Server-Time` when it is there. A page is one instant; between one page and the next the ERP moves on. Tokens count the ERP's clock too (see Token expiry below). Locally: `uv run python -m crestline_erp_api --clock-start 2026-07-03T06:00:00Z --clock-speed 3600`.

**Query:** `GET /erp/api/v2/<Entity>` takes five options; any other `$` option, an unknown field, or a value of the wrong type is a `400` with `{"error": {"code", "message"}}`, and an unknown entity a `404`.
- `$filter`: `<Field> <op> <value>` clauses joined by `and` and `or`, with parentheses (`and` binds tighter), with `op` one of `eq ne gt ge lt le`, on any field: `LastModified gt 2026-07-06T14:30:00Z` for incremental reads, and `JobNum gt 'J1' or (JobNum eq 'J1' and OprSeq gt 20)` for paging by a key of two columns. Values: `'text'` (a quote inside is doubled), numbers, `true`/`false`, `null` (`eq`/`ne` only), dates (`2026-07-06`), and timestamps in whole seconds, `Z` or with an offset. No functions.
- `$orderby`: `Field [asc|desc]`, comma-separated. The primary key is always appended, so rows that tie page in the same order every time; with no `$orderby`, rows come in key order.
- `$top`: the page size. 100 if omitted; anything over 500 is quietly cut to 500.
- `$count`: `true` adds `@odata.count` to the body: how many rows match `$filter` as of the page's instant, whatever `$top`, `$skip`, or `$skiptoken` say. Always a JSON number, even with numbers as strings (that mess is the columns'); `@odata.nextLink` doesn't carry it. Anything but `true` or `false` is a `400`.
- `$skip`: rows to skip. `@odata.nextLink` is present while rows remain and repeats the query with `$skip` advanced by one page. Links write each option's `$` as `%24`, and clients should too: a bare `$` is legal in a query string, but a Lambda function URL rejects it with a `400 InvalidQueryStringException` before the API sees the request.

**Response shape:**
```json
{
  "value": [ { "Company": "CRE", "JobNum": "J104221", "PartNum": "BRK-1042", "RevisionNum": "C", "ProdQty": "250.00000000", "JobReleased": true, "LastModified": "2026-10-02T15:04:11Z" } ],
  "@odata.nextLink": "/erp/api/v2/Jobs?%24skip=500&%24top=500"
}
```
(`ProdQty` as an 8-decimal string is the numbers-as-strings mess; with it off, `"ProdQty": 250`.)

**Behavior (the business reality):**
- Sales enters an order **1–5 working days before** the planner releases its job, so `OrderDate` falls before `ReleaseDate`. Each order has one line, and the line creates one job: routed and costed at entry, with `JobReleased` false and its material required but not issued, until the planner releases it during 1st shift and issues the sheet stock at standard cost.
- Costing is **standard**: `JobCosts` exposes `StdLaborCost`, `StdBurdenCost`, `StdMaterialCost`, and `ActLaborCost`, `ActBurdenCost`, `ActMaterialCost`. Actual labor is the MES labor, one `LaborDetails` row per scan, **rounded to 0.1 hr**, and posted by a **nightly batch** at 22:00 local on the next working day (Friday's labor posts Monday night). **Burden is applied at standard hours earned**: each started operation earns its standard setup plus standard run time for every piece reported, good or scrap, so extra machine time is invisible in ERP.
- Before the batch posts a day's labor, a supervisor works that day's **labor exception report**: a scan that runs past the end of its shift was left open, so its hours are cut back to the shift end. This is the plant's manual workaround for forgotten scan-offs. It stops 16-hour runs from posting, but still over-charges the job for the rest of that shift. The raw 06:00 close stays in the MES files for the data platform to fix better. A forgotten scan is closed at 06:00 the next working morning, before that night's batch, so a job never shows complete in ERP while one of its scans is still open.
- `CompletedQty` on jobs is posted only at job completion (from the final operation), not as parts are made.
- `ProdQty` on a job is the released quantity: the order line's `OrderQty` plus the planner's 5% scrap allowance. Standard cost and material issue are for `ProdQty`; the invoice is for `OrderQty`, or `CompletedQty` if the job came up short.
- Material is issued at standard cost per lb when the job is released (`ActMaterialCost` equals `StdMaterialCost`), not at actual lot cost.

**Messes (§5 has the knobs; each can be switched off on its own).** Some are things the API does to a client when it serves a page; the JSONL export and `erp.db` stay typed and ISO. The rest are things the ERP itself got wrong, so they are in the data, the export included.

What the API does:
- **Numbers as strings** with 8 decimals (`"ProdQty": "250.00000000"`) for every integer and decimal column; booleans, text, and dates are unchanged. Filters still take plain numbers.
- **Legacy dates.** The entities in `legacy_date_format_entities` (`LaborDetails`) return their dates and timestamps as `/Date(1759420800000)/` (a date is midnight UTC). Filters still take ISO 8601.
- **Paging drift.** `@odata.nextLink` pages with `$skip`, so if the ERP changes between pages a read can repeat or skip rows (a sort on `LastModified` shows it best: a nightly batch moves jobs to the front). With `nextlink_drift` off, and the clock running, the link carries a `$skiptoken` instead: the rows to skip and the instant the first page was read at, so every page is served as of that instant. A store that isn't being replayed can't drift and always pages with `$skip`.
- **Token expiry.** A token lives `token_ttl_seconds` of the ERP's clock (never, if null), so a replay at speed outlives it however few wall seconds it takes (this is a replay device; a real token runs on the real clock). `401`, then fetch another.
- **Throttling.** One window per API process on the wall clock: more than `throttle_rps` requests in a second get `429` with `Retry-After` (whole seconds). Requests that fail authorization don't count.
- **Random `500`.** A seeded draw per request, so the same seed and the same requests fail the same way.

What the ERP got wrong:
- **Stale `LastModified`.** An operation's quantities can change without its stamp moving (`JobOperations.qty`), and a `LaborDetails` row can post with the time its scan closed instead of the time it posted (`LaborDetails.post`), so a load by `LastModified` misses both. `Jobs` and `JobCosts` are still stamped when labor posts. `SysValidFrom` in the store still says when each really changed.
- **Duplicate orders, soft-deleted.** A clerk enters an order twice, minutes to a few hours apart (numbered from their own block, `S900001`, `J900001`...). Next working morning, 08:00–10:00, the copy is cancelled: its `SalesOrderLines` row is updated with `Deleted: true` (the order header stays), and its unreleased job, operations, material, and costs are deleted outright. The `Deleted` column is on every line, false unless cancelled. The plant never sees the copy.
- **Job splits.** One to four hours after release, the planner splits a job into `J104221` and `J104221-1`, sharing the quantity out (a quarter to a half goes to the child). The floor keeps running the lot under `J104221`: all the labor is reported there, so the child is released but never worked or completed, and the parent's `ProdQty` (and standard cost and material) is now smaller than what was made. The split is an order-book decision; the plant, the MES, and the machines are unchanged.

### 4.2 MES (`interfaces/mes-api/` and `interfaces/mes-sftp/`)

**What's built, and when.** M1 built the nightly labor file (the backfill writes it to `out/mes/outbound/`), with forgotten scan-offs as its one mess. M3 adds the nightly downtime file below: it is the plant's own record of its downtime, the "before" that the downtime rebuilt from machine data is scored against, and M4's prompt-injection test needs its free text. Deferred, each named in §10: the quality file (with `oven_zone3_drift` and `truth_scrap`, M8, because undercure defects are what it records); the REST interface, `mes.db`, and every file mess below (a later MES ingestion milestone, not yet scheduled). Until then the files are UTF-8 with LF line endings, no trailer, no corrections, and land on time, once.

#### REST (current state)

**Auth:** header `X-Api-Key`.

- `GET /mes/v1/workcenters` — list
- `GET /mes/v1/workcenters/{id}/status` — current job, operator, state
- `GET /mes/v1/jobs/{job_num}/events?since=<local datetime>` — scan events

Field names are `snake_case`. Timestamps are **local time with no offset**: `"2026-10-06 14:31:07"`. No paging; responses are capped at 1,000 events with `"truncated": true`.

#### Nightly files (SFTP)

Written around 02:15 local time to `/outbound/`:

`labor_YYYYMMDD.csv`
```
record_id,record_type,employee_id,employee_name,job_num,op_seq,work_center,scan_on,scan_off,qty_good,qty_scrap,notes
```

`downtime_YYYYMMDD.csv`
```
record_id,record_type,work_center,machine_id,start_time,end_time,reason_text,entered_by
```

The downtime file (M3 step 4) is derived from `truth_machine_states`: one row per stop someone wrote down, on the same nightly schedule as the labor file (a stop goes in the file for the local day it **ended**) and with the same local, offset-free times, typed to the nearest minute. Only unplanned stops (`DOWN` intervals) are candidates; breaks, shift changes, and nights (`PLANNED_STOP`) are never logged as downtime, and a stop the answer key records in chunks around a break is one stop to the operator (`merge_stops` in `sim/systems/mes.py`, which the eval scores against too). What gets written down is tiered: a stop shorter than `min_recorded_downtime_minutes` (5) never (so microstops never are), one of `always_recorded_downtime_minutes` (30) or more always, and one in between is skipped at `downtime_unrecorded_rate` (0.4), because the operator was busy fixing it. `reason_text` comes from the true reason through `free_text_noise`: `none` writes the canonical phrase (`jam`, `material wait`), `high` writes it the way people do (`jam`, `JAMMED`, `jam - see Ray`, `waiting on matl`, `die change??`, `maint`, blank about one time in twenty), with first names from the roster. `entered_by` is the operator's employee ID; a stop with nobody at the machine is written down by the operator nearest in time on that machine and shift. `record_id` is the downtime log's own sequence (from 918,001), assigned in the order the stops ended. Every row is `record_type` `N`, as in the labor file; `C` corrections arrive with the file messes.

`quality_YYYYMMDD.csv` (deferred to M8)
```
record_id,record_type,job_num,op_seq,inspect_time,inspector,qty_inspected,qty_rejected,defect_text,disposition
```

**Human behavior (from truth):**
- Operators scan on/off jobs. MES labor does **not** distinguish setup from run.
- One scan per operator per job operation per shift: on at first touch, off when the operation completes or the shift ends. Operators stay scanned on through breaks and meals, so scanned hours run ~12% above worked hours.
- ~3% of scans are never closed; a supervisor closes them at 06:00 the next working morning, creating 10–16 hour "runs" from 2nd shift (up to a day from 1st shift, and ~60–72 hours over a weekend). Piece counts on those scans are still right (they came off the job traveler); only the time is wrong.
- Scrap is entered as a lump count at scan-off.
- Downtime under ~5 minutes is rarely recorded; microstops never are.
- `reason_text` is free text generated from truth reasons with realistic variation: `jam`, `JAMMED`, `jam - see Mike`, `waiting on matl`, `no matl`, `tooling`, `die change??`, `maint`, blank.
- `defect_text` similarly: `paint peeling`, `adhesion`, `bad finish`, `flatness`, `warped`, `bent wrong`.

**File messes** (deferred with MES ingestion; DST is M3's, see below):
- Windows-1252 encoding, CRLF line endings.
- Trailing summary row: `TOTAL,,,,...`.
- Schema drift: starting day 45, `labor_*.csv` gains a `shift_lead` column in the middle, not the end.
- Corrections: later files contain `record_type=C` rows that replace an earlier `record_id`.
- Late files: occasionally a file arrives a day late.
- Duplicates: occasionally a `_v2` copy of a file is also dropped.
- DST: local timestamps repeat the 01:00–02:00 hour on the fall-back date (2026-11-01 in live mode; configure the backfill window to include a transition when needed). This one isn't a knob; it's what local time without an offset does. M3 tests it with a small fixture (`tests/fixtures/dst/`) of scans planted across midnight, inside the repeated hour, and left open over both clock-change weekends, run through the same views on both engines (`tests/test_dst.py`).

  **The rule: a local time in the repeated hour is its earlier occurrence** (still daylight time: `2026-11-01 01:30` is 07:30Z, not 08:30Z). Found, not assumed: Athena (`with_timezone`) and Python (`zoneinfo`, `fold=0`) read it that way, and DuckDB's own `timezone()` reads it the other way, an hour later; so DuckDB's `with_timezone` macro (`queries/sources.sql`) adds the step that makes it agree, and a test sweeps every quarter hour of 2026 and 2027 against `zoneinfo`. Why the earlier: it's the order the hour happens in, it's what Athena and most libraries already do, and rejecting would drop rows from every query. The cost: a time really in the second `01:xx` reads an hour early, and nothing in the row can tell; a scan from `01:40` to `02:20` that night reads as 1 h 40 when it may have run 40 minutes. Crestline has no shift at 1 AM on a Sunday, so only a weekend callout could land there, and the margin query's `scans` drops those (they fall in no working window). A time that never happens (`02:30` on the spring-forward night) reads as if the clock hadn't moved yet (`02:30` MST, 09:30Z) on both engines.

  **Durations come from UTC.** `mes_labor` gives each scan in both clocks (`scan_on_local`, `scan_on_utc`, ...). Subtracting local times is an hour short for a scan left open over the fall-back weekend (Friday 21:30 to Monday 06:00 ran 57.5 h, not 56.5) and an hour long over the spring-forward one.

### 4.3 Machines (`interfaces/machines/`) — MQTT

**What's built, and when.** M1 built PB-01 and PB-02 with one mess, PB-02's 16-bit rollover. M3 builds all four generations (LSR-01, PB-01, PB-02 and PEM-01, PC-LINE) and every machine mess below.

**Files, not a broker.** The backfill writes each device's messages to `out/machines/<device>.jsonl` (`lsr01`, `pb01`, `pb02`, `pem01`, `pcline`), one line per message as it left the gateway, in `published_at` order: `topic`, `published_at` (UTC, the broker's clock), and `payload`: the JSON object a gateway sent, a base64 string of the protobuf bytes for Sparkplug, or, for a message GW01 corrupted in transit, the truncated text as a string. A reader declares its columns rather than guessing them from the first lines: DuckDB's `read_json` guessed, met the string, typed the whole column as JSON, and the margin query failed to bind (M3 step 4). There is no local broker (decided at M3's scope lock): local pipelines read the files, as in M1 and M2, and in AWS a replay publisher sends the same files to AWS IoT Core in `published_at` order, each gateway with its own certificate (§9). Everything a gateway does in transit (drift, duplicates, outages and out-of-order replays, malformed payloads, resets) is already in the files, so the publisher is dumb: it never decides anything. Machine messes draw from their own RNG streams, so turning one on leaves the truth, the ERP, and the MES labor files byte-identical.

#### LSR-01 — Sparkplug B (modern)

- Topics `spBv1.0/Crestline/{NBIRTH|NDEATH}/EDGE-LASER` for the edge node and `spBv1.0/Crestline/{DBIRTH|DDATA|DDEATH}/EDGE-LASER/LSR-01` for the laser (node-level topics carry no device, per the Sparkplug spec).
- Protobuf payloads per the Sparkplug B spec. The NBIRTH carries `bdSeq` (one higher at every connect, wrapping at 255) and `Node Control/Rebirth`; the NDEATH, which the broker publishes 60 s after the node drops, carries the same `bdSeq` and no `seq`; every other message gets the next `seq` (0 at the NBIRTH, wrapping at 255). The DBIRTH names every metric with its alias; **DDATA carries aliases only**, the way most edge nodes are configured, so a reader needs the last DBIRTH to know that alias 2 is `PartCount` (M3 step 5's SQL does that join). The schema is Eclipse Tahu's `sparkplug_b.proto` (`syntax = "proto2"`, EPL-2.0), kept unmodified with its license header in `interfaces/machines/proto/`; the rest of the repo keeps its own license. Python code is generated from it, and in AWS the IoT rule decodes it from a descriptor set built from the same file (§9).
- Metrics (alias: name): 1 `State` (Int32: 0 idle, 1 setup, 2 run, 3 alarm, 4 hold; a modern controller reports what it is really doing, unlike PB-01's mode), 2 `PartCount` (UInt64, one per piece, kept through a power cycle), 3 `SheetCount` (UInt64: a new sheet at every program change and whenever the parts cut outgrow 70% of a sheet, §2.2), 4 `ProgramName` (String: part, revision, and gauge, `BRK1042C_14GA.nc`, loaded at setup), 5 `AlarmCode` and 6 `AlarmText` (Int32 and String, e.g. 2104 `PART TIP-UP`, 3301 `NC PROGRAM ERROR`, 1207 `ASSIST GAS PRESSURE LOW`; 0 and empty when not in alarm), 7 `CutTime_s` (Float, the last piece's cut time).
- Report by exception: a DDATA with the metrics that changed, everything in one second in one message; plus a heartbeat every 60 s while the laser is on, a DDATA republishing every metric, timed from the DBIRTH.
- The laser is switched on at 05:50 and off at 23:05 plant time on working days (DBIRTH and DDEATH); the edge node stays up. A network outage is the node's NDEATH, then on reconnect an NBIRTH with the next `bdSeq`, a DBIRTH if the laser is on, and the DDATA buffered during the outage, oldest first, every metric flagged `is_historical`, with fresh `seq`s. Births and deaths that fell inside the outage never reach the broker.
- **ProgramName is the practical join key** from machine data to part/rev, and from there to candidate jobs. M3 publishes it but doesn't build the join: with one revision per part (§2.5) there is nothing for it to tell apart yet, so it waits for the revision bumps.

#### PB-01 — flat JSON via GW01 (mid-age)

- Topic `crestline/p10/gw01/pb01/data`
- Published on change, max 1/s:
```json
{ "ts": "2026-10-06T20:31:07.412Z", "N7:12": 2, "CYC_CNT_2": 184233, "B3:0/4": false, "PRG": 1042 }
```
- `N7:12` is state (0 idle, 1 setup, 2 run, 3 fault). `B3:0/4` is the guard door. `PRG` is a numeric program ID.
- `N7:12` reports the mode the operator left the machine in, not whether it is producing: waiting on material, on hold, or between jobs it still reads 2 with no strokes. Jams and program errors read 3, tooling changes 1, and it is switched to 0 for maintenance and at the end of the day. The guard door is open during setup, jams, tooling changes, and maintenance, and interlocked closed while running.
- `ts` is the **device clock**, which drifts.
- Program IDs map to parts only via `reference/brake_program_map.csv`, a "maintained by hand" spreadsheet export with a few missing and outdated rows. Deferred with the parts expansion (§2.5); until then `PRG` is the part number's digits (`BRK-1042` runs `1042`) and the margin query doesn't use it.

#### PB-02 and PEM-01 — polled counters via GW02 (legacy)

- Topic `crestline/p10/gw02/{pb02|pem01}/cnt`
- Every 30 s:
```json
{ "dev": "PB02", "cnt": 65012, "gw_ts": "2026-10-06T20:31:00Z" }
```
- 16-bit counter: rolls over at 65,535. Resets to 0 on a power cycle (`counter_reset_rate`, a seeded share of the machine's working days: three in four at 1st-shift start, the rest at a break or meal, never while running). Where the counter stands when the window opens is seeded close enough to the top that it wraps before the first power cycle, so a 90-day backfill holds a rollover and a reset for the pipeline to tell apart.
- No state, no program, no device timestamp. Gateway timestamps only (`gw_ts`, GW02's clock, shared by both machines).
- GW02 has **no store-and-forward**: data during outages is lost.
- PEM-01 (M3) has the same shape as PB-02 on `crestline/p10/gw02/pem01/cnt` with `"dev": "PEM01"` and its own counter.

#### PC-LINE — process stream via GW03

- Topic `crestline/p10/gw03/pcline/process`
- Every 5 s:
```json
{ "t": 1759782667412, "z1": 398.2, "z2": 401.7, "z3": 399.1, "z4": 395.8, "spd": 12.0, "hook_cnt": 88213 }
```
- Temperatures in °F, speed in ft/min, `t` on GW03's clock (NTP, so true time). No parts, no jobs.
- M3 publishes it with every zone near its setpoint and no scenario: it is the plant's highest-volume tag source, which the cost measurement needs (§9); 1.56 M lines (295 MB) per 90 days. The ovens are lit at 05:00 plant time on working days and warm to their 400 °F setpoints before 1st shift, each zone wandering a degree or two; they are shut off at 23:00 and cool overnight. The conveyor runs at 12 ft/min while the line is working (idle, setting up, running) and stops for a stoppage or a break. `hook_cnt` goes up by one per piece hung, since the plant model has run POWDER one piece at a time since M1. GW03 buffers through an outage and replays in order. `oven_zone3_drift` comes with M8 (§3.5).

#### Machine messes
One knob each in `mess.yaml` (§5), each on its own RNG stream (M3 step 4, `sim/systems/machines/`). The device clocks and counter resets change what a payload says; the rest happen in the gateway, in transit, and the duplicate and malformed decisions are drawn once per message whether or not an outage later swallows it, so switching outages on or off leaves every other decision in place.
- Per-device clock drift (`clock_drift_seconds`, the typical size of the offset): a slowly reverting random walk sampled hourly, PB-01's controller clock most (140 s), GW02's (20 s, stamping both PB-02 and PEM-01), LSR-01's a little (5 s). GW03 keeps NTP time. `published_at` never drifts.
- Duplicate messages (`duplicate_rate`, 0.5%): a QoS 1 redelivery, the same bytes again 0.2 to 2 s later, with the same `seq` for Sparkplug.
- Network outages (`outages_per_week` per gateway, `outage_minutes` 2 to 45): GW01 buffers and drains its buffer **newest first** in a flood 20 ms apart on reconnect; EDGE-LASER is reborn and replays oldest first as historical (above); GW02 loses the polls; GW03 replays oldest first in order. PB-02 and PEM-01, both behind GW02, lose the same polls.
- Malformed JSON from GW01 (`malformed_json_rate`, 0.1%): the message is cut short in transit and arrives as text that will not parse, in place of the message.
- Counter resets on power cycle (`counter_reset_rate`; PB-02, PEM-01), mostly at 1st-shift start (above).

What the margin query makes of it, measured in M3 step 4 on the 90-day backfill: with every machine mess on, DuckDB's reader guesses the schema from the first lines and the one corrupted message makes it fail to bind; with the columns declared, the query runs and places every brake operation, but the drifting PB-01 clock puts 19 of 935 on the wrong brake and reads every setup short (PB-01 setups at 0.78 to 0.91× standard instead of 1.06 to 1.07×; PB-02 2nd shift at 2.53× instead of 2.84×), and the headline gap on PB-02 2nd shift drops from 5.7 to 5.0 points. Alone, drift costs 15 right brakes and resets 4; duplicates, outages, and the malformed message change nothing once the reader survives them, because the query never reads `published_at` and a counter catches up at the next poll. Step 5 rebuilds the machine data properly.

**The rebuild (M3 step 5).** Engine-neutral SQL on top of each engine's sources: `queries/machines.sql` cleans the machine data and counts cycles; `queries/downtime.sql` finds the stops. Sparkplug is decoded before SQL sees it: in AWS by the IoT rule, locally by `crestline_machines.iot_rule`, which writes `out/decoded/lsr01.jsonl` in the rule's own shape (protobuf's JSON mapping) when `crestline query` connects. In order:
- **Dedupe**: a redelivery is the same payload again, so grouping on everything the payload says keeps one of each, at its first arrival. For Sparkplug that is the message type, `seq`, and the payload timestamp: `seq` repeats every 256 messages and `bdSeq` only names the session.
- **Time**: a message that came straight through happened when it was published; its skew (device time less publish time) is the clock's error. One the gateway held through an outage (Sparkplug flags it historical; otherwise its skew is more than 30 s below the fastest message of its publish hour) is put at its own timestamp less the skew of the last message before it, in device time, that came straight through. Interpolating to the first one after was measured and is worse: PB-01 goes quiet at the end of a shift, so "after" is often the next morning.
- **Cycles**: counter deltas in corrected time order (of two counts at one instant, the lower first; of two states, the lower `seq`). A 16-bit counter that goes down rolled over if counting on through 65,535 adds no more strokes than seconds have passed (no machine here makes more than one a second); otherwise it was reset and the strokes are its new value. Per machine per UTC minute and per plant-local day.
- **States**: as intervals, from the state each machine publishes: the laser's `State` (none while switched off), PB-01's mode, PC-LINE's conveyor moving or stopped.
- **Downtime** (`downtime.sql`, after the margin query, whose scans, working windows, and brake attribution it reuses): what the machine says (the laser in alarm or on hold, PB-01 faulted, PC-LINE's conveyor stopped) and stroke gaps between an operation's first and last stroke on PB-01, PB-02, and PEM-01 (PB-02's operations from the margin query's brake attribution), never across a lost GW02 poll, cut to the working windows, each piece at least 3 minutes of working time (a gap that spans a break is mostly the break; M3 step 6 found the slivers either side were two thirds of the presses' stops, all false); overlaps merged. What no machine can name stays with the MES file: the laser shows a tooling change as setup and maintenance as idle, and PB-01 keeps whatever mode the operator left.

The margin query reads its strokes from `machine_counts`, and on the 90-day level-2 backfill its answer table equals the one from M1's clean machine data in every cell: 933 of 935 on the right brake, PB-02 2nd shift 20.5% ERP against 14.8% rebuilt (5.7 points), setups 2.84×, PB-01's 1.06× and 1.07×. Underneath, 11 of 956 brake operations differ from the clean run, each traced to a mess the data can't undo: GW02 outages (strokes counted at the next poll, after their operation ended), the one corrupted GW01 message (its stroke counted at the next good message), and GW01 replays put back to within 46 s.

---

## 5. Mess configuration (`mess.yaml`)

```yaml
seed: 42               # default random seed; `crestline sim backfill --seed` overrides it
mess_level: 2          # 0 = clean, 1 = mild, 2 = realistic, 3 = hostile; per-key values override

erp:
  token_ttl_seconds: 3600       # on the ERP's clock; null = tokens never expire
  throttle_rps: 5               # per API process; null = no limit
  random_500_rate: 0.005
  nextlink_drift: true          # false = nextLink carries a $skiptoken that pins the first page's instant
  lastmodified_skips: [JobOperations.qty, LaborDetails.post]   # [] = honest stamps
  legacy_date_format_entities: [LaborDetails]                  # [] = ISO 8601
  numbers_as_strings: true
  soft_delete_rate: 0.01        # share of orders entered twice and cancelled the next morning
  job_split_rate: 0.02          # share of jobs split into J and J-1 after release
  labor_post_time: "22:00"      # nightly labor batch, local time; null = posts at scan-off
  labor_rounding_hours: 0.1

mes:
  local_timestamps_no_offset: true    # MES REST; deferred with MES ingestion
  forgot_scan_off_rate: 0.03
  min_recorded_downtime_minutes: 5        # the downtime file: a shorter stop is never written down
  always_recorded_downtime_minutes: 30    # one at least this long always is
  downtime_unrecorded_rate: 0.4           # share of the stops in between nobody writes down
  free_text_noise: high                   # reason_text as people write it; none = the canonical phrase
  files:                              # deferred with MES ingestion
    encoding: cp1252
    trailer_row: true
    schema_drift_day: 45
    correction_rate: 0.02
    late_file_rate: 0.03
    duplicate_file_rate: 0.02

machines:
  duplicate_rate: 0.005         # per gateway; a QoS 1 redelivery
  outages_per_week: 2           # per gateway; what happens to the data depends on the gateway (§4.3)
  outage_minutes: [2, 45]
  clock_drift_seconds:          # typical offset of each clock that stamps a payload; GW03 keeps NTP time
    LSR-01: 5
    PB-01: 140
    GW02: 20
  malformed_json_rate: 0.001    # GW01 only
  counter_rollover_bits: 16
  counter_reset_rate: 0.1       # share of a legacy machine's working days with a power cycle
```

**Levels.** `mess_level` sets every knob; any key written in `mess.yaml` overrides it. The values above are level 2. Level 0 turns everything off (no rounding, labor posts at scan-off, no forgotten scans, every stop logged with its canonical reason, no rollover or resets, exact clocks, no duplicates, outages, or corrupted messages, honest stamps, no deletes or splits, a clean API). Level 1 halves every rate (`forgot_scan_off_rate` 0.015, `random_500_rate` 0.0025, `soft_delete_rate` 0.005, `job_split_rate` 0.01, `downtime_unrecorded_rate` 0.2, `duplicate_rate` 0.0025, `outages_per_week` 1, `malformed_json_rate` 0.0005, `counter_reset_rate` 0.05) and allows 10 requests per second; the formats stay as at level 2, and so do the downtime thresholds, the free-text style, how far each clock wanders, and how long an outage lasts. Level 3 doubles the rates (0.06, 0.01, 0.02, 0.04, 0.8, 0.01, 4, 0.002, 0.2), allows 2 requests per second, and halves the token lifetime (1,800 s).

Every mess must be individually testable and individually disable-able.

`mess.yaml` grows with the messes: it lists only messes that are built, and unknown keys are rejected. Built: `mes.forgot_scan_off_rate` and the four downtime-file keys; every `erp` key above; every `machines` key above (M3 step 4 reshaped the spec's `counter_reset_on_power_cycle` into `counter_reset_rate`, because a realized-rate check needs a rate). `mes.local_timestamps_no_offset` and `mes.files` wait for MES ingestion (§4.2). Each mess also has a test that its realized rate lands within three binomial standard deviations of its configured rate, counted from what the interfaces recorded (`tests/test_realized_rates.py`, M3): a seeded year in memory, not the 90-day backfill, because duplicate orders and counter resets are rare enough that a doubled rate still lands inside the 90-day band. The common machine messes are counted on the year's first 90 days (a year of GW02 polls is two million messages); outages are counted per gateway-hour from what each gateway leaves behind (a gap in GW02's polls, a rebirth of the Sparkplug node, a late burst from GW03; GW01's leave no trace at night); the unrecorded downtime is the one count that reads the answer key, since the file cannot know what it skipped. Switched off or doubled, a mess fails its check. A list switch (`lastmodified_skips`) is all or nothing: every affected change stale, or none. `eval/mess_rates.py` prints the same counts month by month for a backfill. The simulator reads the ERP's content messes (`lastmodified_skips`, `soft_delete_rate`, `job_split_rate`, `labor_*`); the API reads the rest from the file named by `CRESTLINE_ERP_MESS` or `--mess` (without one, it serves a clean API, level 0).

---

## 6. Run modes

### Backfill
```
crestline sim backfill --days 90 --end 2026-10-01 --seed 42 --out ./out
```
(`--seed` defaults to the `seed` in `mess.yaml`.) Produces:
- `out/truth/*.parquet` — answer key
- `out/erp/erp.db` — SQLite store served by the ERP API: every version of every row, so the API can serve it as it stands or replay it as of its clock (§4.1)
- `out/mes/outbound/labor_*.csv` and `downtime_*.csv` — the MES nightly files (`mes.db` is deferred with MES ingestion)
- `out/erp/export/*.jsonl` — ERP tables exported as API-shaped records (a one-time bulk historical extract)
- `out/machines/{lsr01,pb01,pb02,pem01,pcline}.jsonl` — raw machine messages as they left each gateway (Sparkplug as base64 protobuf), with publish time and topic (§4.3)

The backfill is the extract as of **06:00 local on `--end`** (exclusive: `--days 90 --end 2026-10-01` simulates Jul 3–Sep 30). By then the last day's MES nightly file (02:15) has landed, but that day's labor has not posted to ERP (it posts in the next working night's batch, §4.1).

With `--upload`, the backfill pushes all non-truth outputs to the raw bucket created by `FoundationStack` (bucket name read from the stack's outputs), using the raw layout in §9. Uploads are idempotent: re-running overwrites the same keys. `out/truth/` is never uploaded.

As built (`src/crestline/upload.py`): the ERP JSONL export, the MES nightly files (labor and downtime), and the machine JSONL go up byte for byte; `erp.db` stays local (Athena can't read SQLite; the export has the same rows). The entity is the file's own name without its date (`Jobs`, `labor`, `downtime`, `pb02`), and `ingest_date` is the extract date, `--end`. A re-run makes that date's folders match the local output exactly: identical files are skipped (MD5 against the object's ETag, so no new versions), changed files replaced, and files the backfill no longer writes deleted, the same rule the backfill applies to its own output folder. Other dates are never touched; versioning keeps anything replaced or deleted for 30 days. Account and region come from `AWS_PROFILE` / `AWS_REGION`.

### Live
```
crestline sim live --speed 1.0 --seed 42
```
Advances the simulation clock in real time (or accelerated with `--speed`), writes to the stores incrementally, drops nightly files on schedule, and publishes MQTT messages live.

Not planned (decided at M3's scope lock). The ERP's replay clock (§4.1) and M3's machine replay publisher (§9) give the AWS side a moving plant from the backfill's files, which is what live mode was for.

---

## 7. Repo layout

```
crestline-supply/
  CRESTLINE_SPEC.md        this file
  README.md
  pyproject.toml           uv-managed; installs the `crestline` package and its CLI
  mess.yaml
  scenarios.yaml
  reference/               parts, routings, customers, rates, brake_program_map.csv
  src/crestline/           the installed package: the simulator plus the small shared library
    sim/                   SimPy plant model, scenarios, truth writer, backfill, and the systems it feeds (ERP, MES, the machines and their gateways)
    reference/             pydantic models and loaders for reference/
    mess.py, rng.py        mess config and the seeded RNG (shared with the interfaces; neither imports sim)
    query.py, cli.py       local DuckDB query runner and the `crestline` command
  interfaces/              one service per folder, each packaged on its own; may import crestline.mess, never crestline.sim
    erp-api/               FastAPI (a uv workspace member: `uv run python -m crestline_erp_api`)
    mes-api/               FastAPI
    mes-sftp/              nightly file writer (+ local SFTP container)
    machines/              the Sparkplug B schema (proto/, Eclipse Tahu, EPL-2.0) with the generated code and the encode/decode module the simulator and the pipelines share (`crestline_machines`), its local stand-in for the IoT rule's decode (`iot_rule.py`), and, from M3 step 7, the replay publisher that sends the backfill's machine files to AWS IoT Core
  pipelines/               ingestion, cleaning, conforming; packaged on its own (a uv workspace member), never imports crestline.sim
    erp_extract/           the ERP extractors, stdlib only: naive.py (the M2 "before", measured) and robust.py; raw.py writes pages, merge.py replays them; curated.py, curated.sql and curate.py build the curated tables; `python -m erp_extract` runs one pass
  queries/                 SQL: sources.sql, machines.sql (the machine data cleaned, M3), and one file per question (margin_gap.sql, downtime.sql); DuckDB locally, Athena in AWS
    athena/                tables.sql (raw external tables) and sources.sql (the same source names as DuckDB's), so every question file runs unchanged on both engines
  eval/                    reconciliation scoring vs. truth: the only code outside sim/ and tests/ that reads out/truth/
                           (extraction.py scores an ERP extractor, and curated.py the curated tables, against the ERP's own state, not the answer key)
  infra/                   AWS CDK (Python): one app, one stack per layer — FoundationStack in M1, ingestion and agent stacks added per milestone
  tests/                   mirrors src/crestline/, plus the truth-isolation check
  out/                     gitignored
```

The simulator lives inside the installed package because the CLI and the tests import it. Everything that ships to AWS or runs as a service lives at the top level in its own folder, so it can be packaged without the simulator and cannot import it by accident. `tests/test_isolation.py` checks the rule mechanically.


---

## 8. Tech stack

- Python 3.12, `uv`, `ruff`, `pytest`
- `simpy`, `pydantic`, `polars` / `pyarrow`, `pyyaml`
- `fastapi`, `uvicorn`
- `paho-mqtt` (the replay publisher, to AWS IoT Core); Sparkplug B protobuf generated from Eclipse Tahu's `.proto`
- No Docker Compose or local broker (dropped at M3's scope lock; nothing has needed them)
- AWS CDK in Python

---

## 9. AWS target architecture (later milestones)

- **Foundation (M1):** raw data bucket, Athena results bucket, Glue database, Athena workgroup with a scan limit, cost allocation tags from day one. As built (`infra/stacks/foundation.py`, stack `CrestlineFoundation`): both buckets S3-managed encryption, public access blocked, HTTPS only; the raw bucket is versioned and replaced versions expire after 30 days; query results expire after 7 days; Glue database `crestline_raw`; workgroup `crestline` (engine v3) with a 1 GB per-query scan limit and enforced settings. Stack outputs name the buckets, database, and workgroup. The stack is environment-agnostic (account and region come from the AWS profile) and `cdk destroy` empties and deletes both buckets. A Glue database can't carry tags in CloudFormation; the Data Catalog is free at this size, so the cost report misses nothing.
- **Machines (M3):** MQTT → AWS IoT Core (Basic Ingest) → IoT rules → Amazon Data Firehose → S3 raw → curated Iceberg tables
  - `MachineIngestStack` (`layer=machine-ingest`, `milestone=M3`): one IoT thing per gateway (`EDGE-LASER`, `GW01`, `GW02`, `GW03`), each with a policy that lets it publish only to its own Basic Ingest topics (`$aws/rules/<rule>/...`), which skip the message broker and its messaging charge; rules triggered and actions run still bill. One rule per generation adds `topic()` and `timestamp()` (as `received_at_utc`); the Sparkplug rule keeps the raw bytes as base64 and also decodes them with `decode(*, 'proto', ...)` against a descriptor set in S3. Tested in M3's spike (us-west-2): `decode(*, 'proto', '<bucket>', '<key>.desc', 'sparkplug_b', 'Payload')` reads the proto2 schema as it is, extension ranges and `oneof` included, with the descriptor set built by `protoc --include_imports --descriptor_set_out` and the bucket policy letting `iot.amazonaws.com` read it. The output is protobuf's JSON mapping: camelCase field names (`longValue`, `stringValue`, `intValue`), unset fields left out, and 64-bit integers as strings (`"longValue": "184233"`, exact up to 2^64), 32-bit ones as numbers. `topic()` is the original topic without the `$aws/rules/<rule>/` prefix. **A payload `decode()` can't parse is dropped silently**: it counts in the rule's `TopicMatch` metric but reaches neither the action nor the error action, so the error action alone can't account for every message (decided when the stack is built). Each rule feeds a Firehose stream writing `s3://<raw>/source=machines-iot/entity=<device>/ingest_date=YYYY-MM-DD/` with a plain timestamp prefix (no dynamic partitioning), and has an error action to a countable `errors/` prefix, so every message published is in raw or in errors.
  - As built (M3 step 7, `infra/stacks/machine_ingest.py`, stack `CrestlineMachineIngest`; the device list is `crestline_machines/devices.py`, shared by the stack, the publisher, and the accounting): **a rule and a Firehose stream per device**, not per generation, because a stream writes one fixed prefix and GW02 carries two devices. Each rule keeps `topic()`, `timestamp()` as `received_at_utc` (epoch ms), and two MQTT 5 user properties the publisher sets, read with `get(get_user_properties('<key>'), 0)` (plural; the function returns an array): `published_at`, the plant time the message left the gateway (drift correction needs it; in a replay `timestamp()` is the replay's wall clock) and `replay`, the run's name. JSON devices keep their fields (`SELECT *`); the Sparkplug rule keeps `payload_b64` and `decoded`, the same record `crestline_machines.iot_rule` writes locally. Firehose buffers 60 s and writes uncompressed newline-separated `.jsonl`; its own failures go to `source=machines-iot/firehose-errors/`, a rule's error action to `source=machines-iot/errors/entity=<device>/ingest_date=<date>/<uuid>.json`. **A JSON message that won't parse reaches neither**: the rules engine parses a JSON-looking payload before any SQL runs, whatever the SQL (`SELECT *` and `encode(*, 'base64')` alike, measured), drops it, and counts it only in the rule's `ParseError` metric; so a message published is in raw, in errors, or in that metric, and the accounting reads all three. Not every corrupted message is dropped (measured on the 90-day replay, M3 step 8): of GW01's 29, the 21 cut inside a token (mid-string, mid-`false`) were, and the 8 cut between tokens (`{"ts":"...Z"`, `"N7:12":2,`) passed the parser and landed as lines with the topic, times, and replay but none of the payload's fields; they count as raw, and Athena's `pb01_data` skips them (no device time), as DuckDB's skips every corrupted line. The Sparkplug descriptor set (`interfaces/machines/proto/sparkplug_b.desc`, built with `protoc --include_imports --descriptor_set_out` and committed) sits in the stack's schema bucket, put there by the setup command rather than a CDK BucketDeployment, whose Lambda's log group outlives the stack and which doesn't empty a bucket its own stack owns.
  - Certificates: CloudFormation can't hand out a private key, so `crestline machines setup [--delete]` creates one per gateway through the API (attached to its thing and policy; keys in the gitignored `.secrets/machines/<gateway>/`, owner-only) and uploads the descriptor set; `--delete` removes both, working from what AWS says is attached, and must run before `cdk destroy`. `crestline machines publish --from --to --speed N [--devices] [--limit]` replays the backfill's machine files for plant-local days FROM to TO in `published_at` order over MQTT 5/TLS (QoS 1, Basic Ingest), each gateway with its own certificate, byte for byte as they left the gateway, at most 90 messages a second per connection (IoT Core allows 100), reconnecting after a drop and recording it; it writes a manifest to `out/replays/<name>.json`. `crestline machines account [--replay]` checks, per device, sent = raw - redelivered + errors + parse errors, and that every message was acknowledged. A redelivery is QoS 1 doing its job: a message in flight when a gateway's connection drops is sent again on reconnect and can land twice (the step 10 replay from a fresh clone: GW02 and GW03 each dropped once, and 3 PEM-01 polls and 1 PC-LINE sample landed twice). A copy is the same line again but for `received_at_utc`; the backfill's files never repeat a publish time and payload, so any repeat in raw is one. `machines.sql` drops copies like any other duplicate, and the curator's `select distinct` merges a copy received in the same millisecond as one row. AWS replays a few days; the 90-day numbers come from the local run on the same files.
  - Curated machine tables (M3 step 8): one Iceberg table per device in `crestline_curated` (`machine_pb01`, ... `machine_lsr01`; CuratedStack), typed, with the source views' column names (`device_ts_utc`, `gateway_ts_utc`, `published_at_utc`, `received_at_utc`, `replay`), the laser's `decoded` kept as JSON text; columns and expressions in `pipelines/machine_curate/tables.py` (Iceberg type names: `long`, not `bigint`). Nothing is deduplicated: a row's key `message_id` is the SHA-256 of its landed line (a topic and publish time don't identify a message: the laser's heartbeat and a data message share a millisecond hundreds of times), so re-merging a file inserts nothing twice, and `machines.sql` deduplicates on both engines. The curate step (`machine_curate/curate.py`, its own Lambda in MachineIngestStack, hourly by EventBridge Scheduler, one run at a time) reads Firehose's files through `raw_machines_iot` (one line of text each, projected by device and arrival date), from the file after `merged_through` (Firehose names files by time, so keys sort in arrival order; a file younger than 2 minutes waits, with every file after it), checks Athena sees exactly the lines S3 holds, and runs an insert-only MERGE (`when not matched then insert`). The guard: a table never shrinks; if a MERGE leaves fewer rows than before, the step emails the alarm topic and fails without saving `merged_through`. Daily OPTIMIZE and VACUUM; a Lambda error raises a CloudWatch alarm to the stack's SNS topic (`-c machineIngestAlarmEmail=...`). Athena's machine sources read each table's newest replay, the same newest-extract rule as the raw tables, so a 90-day replay is what every Athena check reads.
  - Data-shape checks (`pipelines/checks/`, M3 step 8): after every machine curate run, and as the ERP poller's last step once every entity is merged (the ERP's reconciliation needs `LaborDetails` and `JobCosts` merged to the same run). Three kinds: each table's key columns' null rate against its own last 7 runs (a breach is more than one percentage point above their maximum; the first run records a baseline; baselines on the state table's `_checks` item); primary key uniqueness; and one reconciliation total per fact table: ERP, each job's posted `LaborHrs` at its work centers' labor rates is its `ActLaborCost` to the cent (exact on all 1,309 jobs); machines, each plant day's rebuilt brake strokes are the pieces scanned at BRAKE on scans opened that day, within 2 strokes or 1% (exact on all 64 days; by scan-off date it misses by up to 21%, because a forgotten scan is closed the next morning). A breach fails the run, so its alarm fires; the machine curator also emails what it found.
  - Cost per 1,000 tags is measured, not extrapolated: an opt-in one-hour load test (`layer=machine-loadtest`) at 1,000 tags and 1 Hz, once with one message per tag change and once batched per gateway, read from the bill.
  - Not used, because AWS has retired or closed them: AWS IoT Analytics (end of support Dec 15, 2025), AWS IoT Events (end of support May 20, 2026), and Amazon Timestream for LiveAnalytics (closed to new customers June 20, 2025). Recheck these dates before publishing.
- **The ERP itself, in AWS (M2 step 6):** `ErpApiStack` (`CrestlineErpApi`, `layer=erp-api`) runs the same FastAPI app in one Lambda function (Python 3.12, arm64, 512 MB) through Mangum, behind a function URL that is open to the internet: the ERP's own OAuth token endpoint is the authentication. The package is built on the deploying machine with uv (Linux arm64 wheels pinned by `uv.lock`), holds the API, `crestline/mess.py` and `rng.py` (not the simulator), and the backfill's `erp.db` (read-only), so nothing runs between requests. The client ID is `crestline-poller`; the client secret and signing key are SecureString parameters (`/crestline/erp-api/client-secret`, `/crestline/erp-api/signing-key`) that `crestline erp-api secrets` creates with random values before the first deploy, because CloudFormation can't create a SecureString without writing its value into the template; the function reads them once per cold start. Deploy-time context chooses the behaviour: none serves the store as it stands; `erpApiMess` adds a mess file; `erpApiClockStart`/`erpApiClockSpeed` (and `erpApiClockAnchor`, pinned at synth if not given) replay the history, the same in every copy of the function. The log group keeps a week and is deleted with the stack; the parameters are not (`crestline erp-api secrets --delete`).
- **ERP:** EventBridge Scheduler → Step Functions → Lambda poller (tokens, paging, throttling, retries, watermarks) → S3 raw
  - As built (`pipelines/erp_extract/robust.py`, M2 step 5; runs locally first): sign in again on a `401`, wait out a `429`'s `Retry-After`, back off exponentially with jitter on a `5xx` (up to 8 tries), and drop a read that still fails whole (nothing written, watermark unmoved). Skip an entity if the ERP's time at sign-in is no later than the last run that read it: nothing can have changed, a run is named for that time, and reading again would reuse the last run's files; a replay that has stopped then writes nothing. Every read pages by key with `$orderby` on the primary key and a key filter, never `$skip`. Master data: a full read every run. `LaborDetails` (insert-only): by sequence, `LaborDtlSeq gt <highest read>`. Orders and lines: `LastModified gt <the ERP's Date when the last complete read began, less 1 hour>`. `Jobs`, `JobOperations`, `JobMaterials`, `JobCosts`: the same incremental read, plus a sweep (a full read) every night, because their `LastModified` misses operation quantities and deleted duplicates.
  - A full read or a sweep has to prove it is complete before it may delete anything, because a read the ERP ends early with a clean `200` (no `nextLink`, or an empty page) looks finished. Its first page asks `$count=true`, and once its last page is in it asks again (`$top=1&$count=true`). It is complete only if the distinct keys it collected fall within `[min(start, end) - SLACK, max(start, end) + SLACK]` (`SLACK` = 10, far below the 500-row page, so a lost page can't hide in it; a busy ERP can insert ahead of the cursor and delete behind it. Measured in M3: reading at 09:00 plant time at 5 ERP minutes a page, across order entry, releases, and the 08:00 to 10:00 cancellations, every read was complete and the biggest gap was 0, because new jobs and cancelled duplicates both sit at the end of the key order, ahead of the cursor), and, if its last page held a full 500 rows with no `nextLink`, one more request for the rows after its last key comes back empty. An ERP that sends no count can't prove it. A read that fails the check is still written, as `shortfull` or `shortsweep` (one word: the kind is the name's last hyphenated part), so its rows upsert but it deletes nothing; the counts go in its last page's `check` field. The Lambda then fails the entity, so Step Functions retries it and the alarm fires if the retries come up short too.
  - Raw pages: `source=erp-api/entity=<name>/ingest_date=<run date>/<run>-<nnn>-<read>.jsonl`, one file per completed read (written under a temporary name, then renamed), one line per page: `entity`, `run` (the ERP's time at sign-in, `20260706T050000Z`), `read` (`full`, `incremental`, `sequence`, `sweep`, or `shortfull`/`shortsweep` for a complete read that failed its check), `page`, `url`, `requested_at` (the extractor's clock), `server_time` (the ERP's `Date`), `body`, untouched, and `check` (on a full read's or sweep's last page: the counts at start and end, the keys collected, the rows found after a full last page, and whether it was short; otherwise null). State between runs (watermarks, sequence, run count) is a small JSON document.
  - In AWS (M2 step 7): `ErpIngestStack` (`CrestlineErpIngest`, `layer=erp-ingest`). An EventBridge Scheduler schedule (hourly; `-c erpIngestSchedule=...`) starts a Step Functions standard workflow that maps over the 11 entities, three at a time, invoking one Lambda per entity (`erp_extract/aws.py`, the robust extractor for one entity; Python 3.12, arm64, 256 MB; the package is `pipelines/erp_extract`, nothing else). An entity that raises `EntityFailed` (a read dropped) is retried twice, 30 s then 60 s later, then caught so the others finish, and the run ends `Failed` naming it (a JSONata `Choice`). Per-entity state (runs, watermark, sequence, last run) is one item each in an on-demand DynamoDB table. The extractor reads the API's URL (`/crestline/erp-api/url`, a plain parameter ErpApiStack publishes, so the two stacks never depend on each other) and the client secret from Parameter Store, and may write only `source=erp-api/*` in the raw bucket, which it gets from FoundationStack as a weak cross-stack reference (`Fn::GetStackOutput` on two published outputs, no export). Behind the function URL it takes the ERP's time from `X-Server-Time`. EventBridge Scheduler starts the first run as soon as the schedule is created. An EventBridge rule sends every run that fails, times out, or is stopped to an SNS topic; `-c erpIngestAlarmEmail=...` subscribes an address at deploy time.
  - The copy is the raw reads replayed in order (`merge.py`): increments upsert by key; a full read or a sweep is a complete picture, so keys it lacks are deleted. The curated layer's MERGE does the same in SQL.
- **ERP change data capture (CDC): not built** (M2 step 9). Reading the ERP database's log (e.g. DMS from an RDS replica) would catch every write and delete within seconds, but it needs database-level access that a mid-market ERP's support contract (or a cloud ERP) often doesn't allow, and an always-on replica and replication instance. The API poller with nightly sweeps gets the same copy (M2 step 8: 0 rows wrong) at about zero idle cost, so CDC is compared in writing, not in code.
- **MES files:** AWS Transfer Family (SFTP) → S3 raw; MES REST polled like ERP
- **Raw layer:** immutable, partitioned `s3://<raw-bucket>/source=<erp|mes|machines>/entity=<name>/ingest_date=YYYY-MM-DD/`. The backfill's bulk export is `source=erp` (and its machine files `source=machines`); the ERP poller's pages are `source=erp-api` (below) and IoT Core's messages `source=machines-iot`, so the two shapes never share a prefix and the backfill's prune (which lists only its own sources) never touches the poller's files.
- **Curated layer:** Iceberg tables in the Glue Data Catalog, built with Athena; handles corrections, dedupe, time normalization to UTC, counter rollover, and joins
  - As built for the ERP (M2 step 8): `CuratedStack` (`CrestlineCurated`, `layer=curated`) holds a curated bucket and the Glue database `crestline_curated` with one Iceberg table (format version 2) per ERP entity, `erp_<entity>` (`erp_jobs`, `erp_job_operations`, ...). Columns are the ERP's field names in lower case (`jobnum`), and a timestamp also says its clock (`lastmodified_utc`, from M3 step 1; a table created before that needs `crestline curate --rebuild` after redeploying), typed from `erp_extract/curated.py`: whole numbers `int`, decimals `double`, true/false `boolean`, dates `date`, timestamps UTC `timestamp`; key columns are required. A Glue database and table can't be tagged in CloudFormation; the bucket is. S3 Tables was tried first and dropped: Athena reaches a table bucket through a federated catalog and won't create a view over it, and the queries read every source through views.
  - The **curate step** runs in the poller's workflow right after each entity is extracted (`erp_extract/curate.py`, its own Lambda; `crestline curate` runs it from a laptop). It lists the entity's raw reads after `merged_through` (kept on the entity's DynamoDB state item), starts the window at the newest full read or sweep among them (or the first read, if none), checks the window (`erp_extract/curated_check.sql`), and runs one Athena `MERGE` (`erp_extract/curated.sql`) for that window. The check fails the step, before anything changes, unless Athena sees exactly as many rows in the window's raw reads as their files hold (counted from S3) and the MERGE would delete at most 5% of the table (`crestline curate --allow-mass-delete` lifts the limit). The MERGE: the newest version of each key wins, its text values are typed (`"250.00000000"`, `/Date(ms)/`), unchanged rows are left alone, and if the window starts with a complete read, keys it lacks are deleted. If the window starts complete (so the MERGE could delete), the step reads the table's current Iceberg snapshot ID before the MERGE, from the metadata file Glue's `metadata_location` points to (free, where an Athena `$snapshots` query bills 10 MB), and after it runs `erp_extract/curated_deleted.sql`: the keys `FOR VERSION AS OF` that snapshot holds that the table no longer does. It writes them to `s3://<curated bucket>/audit/merges/entity=<Entity>/<window's last read>.json` with the window, the snapshots before and after, the rows held, and the predicted and actual deletes; a table with no snapshot yet skips it. Then it saves `merged_through`. If the actual count differs from the check's, the audit goes to `<last read>-mismatch.json` instead, the step emails the alarm topic (SNS, from the curator directly) and fails without saving `merged_through`. When the 5% check refuses a MERGE, the step first runs `erp_extract/curated_gone.sql` (the check's anti-join as a key list) and writes `<last read>-refused.json`: the proposed deletes and the table's row count, the limit, the table's current snapshot ID, the window's raw reads by name and S3 key, the complete read that would trigger the deletes, every key, and the command that accepts it; then it emails the alarm topic with the count, snapshot, read, and file, and fails. Ordinary deletes are recorded, not emailed. The audit runs before the day's upkeep, whose `VACUUM` expires old snapshots. Athena reads the raw pages through `raw_erp_api` (ErpIngestStack): one line of text per page, partitioned by entity and ingest date with partition projection over 2020-01-01 to 2099-12-31 (`RAW_INGEST_DATES`: a page filed outside the range is invisible, which once emptied the tables), and the MERGE names the window's dates, so it scans only those files. Once a day per table (`upkept_on`) it runs `OPTIMIZE ... REWRITE DATA USING BIN_PACK` and `VACUUM`. `--rebuild` merges from the first raw read, rebuilding a table from the raw bucket alone.
  - Soft-deleted sales order lines stay in `erp_sales_order_lines` with `deleted` true, as in the ERP; readers filter. Rows a sweep finds gone (cancelled duplicates' jobs) are deleted.
  - Athena's `erp_*` source views (`queries/athena/sources.sql`) read the curated tables; the backfill's export tables (`raw_erp_*`) stay for comparison.
- **Query:** Amazon Athena
- **BI:** Amazon Quick (later)
- **Agent:** Bedrock AgentCore with read-only MCP servers over curated data; Cedar policies; no network path to OT

---

## 10. Milestones

| # | Scope | Done when |
| --- | --- | --- |
| M1 | Sim core, reference data, scenario framework + `pb02_second_shift_setups` only, truth writer, backfill mode writing ERP (SQLite + JSONL export), MES labor CSV, and PB-01/PB-02 counter JSONL; local queries with DuckDB; CDK `FoundationStack`; automatic upload to S3 raw; the same margin query in Athena. | A query over ERP + MES + machine data (not truth) shows PB-02 2nd-shift jobs with ERP margin materially higher than reconstructed actual margin; Athena results match local results; the eval confirms it against truth |
| M2 | Full ERP API with all messes; ERP poller; raw → Iceberg | Poller extracts a complete, deduplicated, correct job/cost history under `mess_level: 2`, verified against truth |
| M3 | All four machine generations with every machine mess (§4.3); the MES downtime file (§4.2); `_utc`/`_local` timestamp names and an Athena round-trip time zone test; a DST fixture; realized-rate checks for every mess; cycles and downtime rebuilt from machine data in engine-neutral SQL; IoT Core Basic Ingest → rules → Firehose → raw → curated, fed by a gateway replay; data-shape checks; cost per 1,000 tags measured. No new scenarios, parts, or local broker (§3.5, §2.5, §4.3) | Reconstructed cycle counts per machine per day within 2% of truth; downtime recall and precision reported by machine and generation, next to the MES downtime file's; the margin query on the messy machine data measured, then matched on the rebuilt data or every difference explained; a replay accounted for message by message (published = raw + errors); Athena equals DuckDB on the replayed window |
| M4 | Read-only MCP server over curated data; Cedar policies; prompt-injection test via MES notes | Injection attempts fail at the policy layer, demonstrated in a test |
| M5 | One-command CDK deploy; morning brief batch path; eval set of ~20 plant-manager questions; `laser_nozzle_degradation` as a morning-brief anomaly (moved from M3) | Deploys cleanly in a fresh account; eval results published |
| M6 | Add `bad_material_lot` scenario; lot genealogy (lot → job → parts, both directions) | Bad lot's affected jobs identified with no false negatives |
| M7 | Cost allocation tags, cost report, simple calculator | Quarter's actual AWS costs broken down by layer |
| M8 | `oven_zone3_drift`; the MES quality file and `truth_scrap`; undercure found at ASSY inspection 1–2 days after coating (moved from M3) | Undercure scrap traced back to the oven window it came from |
| — | The parts expansion to ~20, revision bumps, `brake_program_map.csv`, and the `ProgramName` join (§2.5); MES ingestion: the REST interface, `mes.db`, and the file messes (§4.2) | — |

### Reconciliation metrics (`eval/`)
- Downtime recall and precision by reason category
- Cycle-to-job attribution accuracy
- Scrap attribution accuracy (by job and by cause)
- Job actual cost error vs. `truth_costs`

M1 builds the last of these: `eval/margin_gap.py` scores the margin query's rebuilt cost per finished job against `truth_costs` (share within 1% and 5%, with the ERP's own actual cost alongside), names the reason for every miss of 1% or more, checks brake and setup-shift attribution against `truth_operations`, and puts true margin next to ERP and rebuilt margin by brake and setup shift.

M2 adds `eval/extraction.py`: it runs an ERP extractor every night across the window against the messy, changing API (in process, on a clock the harness controls, so it is deterministic) and compares what the extractor holds with the ERP's final state, row for row, by entity: rows **missed**, **wrongly deleted** (once held, since removed), **stale**, **kept deleted** (the ERP deleted them; the copy didn't), and **pulled twice** within one read. The naive extractor (`pipelines/erp_extract/naive.py`) is the "before", scored on the copy it keeps; the robust one is the "after", scored on the copy rebuilt from the raw pages it wrote, so the score is of what it delivered. `--extractor naive|robust|both`. A control run on an ERP with nothing wrong must score zero errors.

M3 also adds `eval/mess_rates.py`: how often each built mess really happened, month by month, from a backfill's files and ERP store (and, for the downtime file alone, the answer key), against its configured rate, the machine messes and the downtime file included from step 4; and `eval/extraction.py --nightly-at` for reading while the ERP is busy.

M3 adds `eval/machines.py` (local only): cycles per machine per day against `truth_cycles` (the share of machine-days within 2%, every miss given a cause), next to the margin query's arithmetic on the raw messages (counter differences in device-timestamp order, mod 65,536 for a 16-bit counter); and downtime recall and precision against `truth_machine_states` by machine, generation, and duration band (under 2 minutes, 2 to 5, over 5, by a stop's DOWN time), for the downtime rebuilt from machine data, the MES downtime file, and the two together (overlaps merged), so the plant's own record and the rebuild are scored on the same basis. A true stop is `merge_stops`' (as the operator saw it) and is found if a source stop on its machine overlaps it; a source stop is right if it overlaps a true DOWN chunk. Hours are scored in working time: an operator writes a stop from when the machine quit to when it ran again, so the file as written counts the nights and weekends a stop ran across (both are reported). Every missed true stop is given a reason (the laser shows tooling as setup and maintenance as idle; too short for the 3-minute gap rule; outside an operation's run; across lost GW02 polls; under 5 minutes or not written down, for the file). Downtime by reason category waits for the MES reason text to be cleaned, which M3 doesn't do. A control run at mess level 0.

---

## 11. Non-goals

- Not a general-purpose manufacturing simulator.
- Not a faithful replica of any vendor product.
- No UI in the simulator beyond logs and CLI output.
- No write paths from the agent to any machine or system of record, ever.

## 12. Open decisions

- Accelerated time factor for live mode in AWS (real time vs. 10×) given the cost.
- Whether to publish the answer key in the public repo or generate it only locally.
- Whether to add a second plant for the capstone.
- Whether one machine layer gets a real Ignition trial instance for credibility.
