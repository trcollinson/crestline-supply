"""The plant's systems: what the MES, the ERP, and the brake controllers record.

Each system sees only its own slice of what really happened, and each one is
derived from something it could plausibly observe:

- the **MES** from operators scanning on and off jobs (truth labor and
  pieces made);
- the **ERP** from its own orders and jobs, the standard routings, and the
  labor the MES posts to it. It never sees the simulation itself;
- the **brake controllers** from their own state and stroke counter (truth
  machine states and pieces), with no idea which job is on the machine.

These live under `sim` because producing them needs the answer key. The
services that serve them from M2 on (under `interfaces/`) read only the
files written here.
"""
