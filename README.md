# Crestline Supply

[![CI](https://github.com/trcollinson/crestline-supply/actions/workflows/ci.yml/badge.svg)](https://github.com/trcollinson/crestline-supply/actions/workflows/ci.yml)

A simulated mid-market metal fabrication plant, the messy data its systems produce, and an AWS data platform that joins them.

Mid-market manufacturers have three data sources that never meet. The ERP knows the money, but costs every job at standard. The MES knows what people scanned and typed. The machines know what actually happened, but not which job it was. The value lives in the join, and so does the difficulty.

Crestline Supply is a fictional plant built to work on that join honestly. A simulator records what really happened on the floor, then writes what the ERP, the MES, and the machine gateways would each have recorded: three partial, distorted views of the same truth. Queries run over those views and never see the truth. An eval scores them against it.

## The first question: is the ERP right about margin?

Press brake PB-02's setups on second shift take almost three times standard. The ERP applies machine burden at standard hours, so it never notices. Rebuilding each job's cost from MES scans and brake stroke counters does.

| Brake, setup shift | ERP margin | Rebuilt | True |
|---|---|---|---|
| PB-01, 1st | 24.3% | 25.5% | 27.1% |
| PB-01, 2nd | 25.9% | 27.8% | 28.6% |
| PB-02, 1st | 23.0% | 23.7% | 25.0% |
| **PB-02, 2nd** | **20.4%** | **14.8%** | **16.0%** |

Rebuilt job cost lands within 1% of the true cost for 92% of jobs. The ERP's own "actual" cost manages that for 15%.

## How it fits together

```mermaid
flowchart LR
    subgraph sim["simulator"]
        plant["SimPy plant<br/>90 days, seeded"] --> truth["answer key<br/>out/truth/"]
        truth --> erp["ERP export"]
        truth --> mes["MES labor files"]
        truth --> brakes["brake gateways"]
    end
    erp & mes & brakes --> duck["DuckDB, local"]
    erp & mes & brakes -->|upload| s3["S3 raw bucket"]
    s3 --> athena["Athena over Glue tables"]
    duck & athena --> query["queries/margin_gap.sql"]
    query --> eval["eval/"]
    truth -. "read only by the eval" .-> eval
```

## Run it locally

Needs Python 3.12 and [uv](https://docs.astral.sh/uv/). Run from the repo root.

```sh
uv sync
uv run crestline sim backfill --days 90 --end 2026-10-01 --seed 42
uv run crestline query margin-gap
uv run python eval/margin_gap.py
```

The backfill takes a few seconds and writes everything to `out/`: the answer key in `out/truth/`, and the ERP export, MES files, and brake messages beside it. Same seed, same bytes, every time.

To watch the story switch off, set `enabled: false` under `pb02_second_shift_setups` in `scenarios.yaml` and run the backfill and the query again. PB-02 second shift falls in line with the other groups.

## Run it on AWS

The same query runs in Athena over the same files in S3. You need Node.js with the CDK CLI (`npm install -g aws-cdk`, 2.1143 or later) and an AWS profile with a region configured. Run from the repo root.

```sh
export AWS_PROFILE=<profile>
uv sync
(cd infra && cdk bootstrap && cdk deploy)
uv run crestline sim backfill --days 90 --end 2026-10-01 --seed 42 --upload
uv run crestline athena setup
uv run crestline query margin-gap --engine athena
```

`cdk bootstrap` is needed once per account and region. `cdk deploy` asks once to approve the IAM role it creates. The stack is two S3 buckets, a Glue database, and an Athena workgroup capped at 1 GB per query. Idle, it costs next to nothing; the margin query scans about 100 MB, a twentieth of a cent.

To take it down, `cdk destroy` in `infra/` empties the buckets and deletes the stack. It leaves one thing behind, the log group of the Lambda that emptied the buckets:

```sh
aws logs describe-log-groups --log-group-name-prefix /aws/lambda/CrestlineFoundation --query 'logGroups[].logGroupName'
aws logs delete-log-group --log-group-name <name from above>
```

## Layout

| | |
|---|---|
| `src/crestline/sim/` | the SimPy plant, the hidden scenario, and the ERP, MES, and brake gateway writers |
| `reference/` | parts, routings, rates, customers, employees, all fictional |
| `queries/` | the SQL, one file for both engines; `athena/` holds the raw tables and views |
| `infra/` | the CDK app |
| `eval/` | scoring against the answer key |
| `docs/SPEC.md` | the design: the plant, its systems, the messes, and the milestones |

One rule holds it together: the query, upload, and Athena code never reads `out/truth/` or imports the simulator. Only the eval and the tests may. A test enforces it.

## Status

Milestone 1 of 7: the plant, one hidden scenario, four messes, the margin query, and the AWS foundation. Next comes the full ERP API with its messes and an ingestion poller. The roadmap is in `docs/SPEC.md`.

## License

Apache 2.0. Copyright 2026 Tim Collinson.
