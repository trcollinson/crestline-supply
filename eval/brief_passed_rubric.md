# Reading a passed morning brief

The rubric the first read of `eval/brief_passed_review.yaml` followed (M5 step
9), as given to each reviewer but for where its packets were kept.
`eval/brief_passed.py sample` shows briefs the same way, each beside its
morning's facts.

You are reading morning briefs that a model wrote for a plant manager at a
fictional metal fabrication plant. Each brief passed an automatic number check:
every number in it equals (as given or rounded) a fact cited in the same
paragraph or bullet. The check reads only numbers. Your job is everything else:
find each statement that would leave the plant manager believing something the
facts don't say.

Each packet holds the brief as delivered (the log's reply, citations unescaped)
and, below it, the facts the model was given (the log's request): each fact's
ID (`family:subject:metric`), value, baseline (or, for an alert, its
threshold), unit, and, for alerts, their status. The plant day is the working
day before the morning (Friday, on a Monday). The model was told: lead with the
alerts, new ones first, each with its status as given; then output, margin,
stops, scrap; numbers only from the facts; no causes, advice, or actions.

What the metrics mean:
- pieces: pieces made (good plus scrapped); its baseline is the usual day's
- good, scrapped: good pieces, pieces scrapped
- scans: operator scans on and off jobs
- jobs_finished: jobs whose last operation finished on the plant day
- jobs: jobs the ERP closed in the margin window
- erp_margin_pct: margin as the ERP costs it; rebuilt_margin_pct: margin rebuilt from the machines' real times
- gap_points: ERP margin minus rebuilt margin, in percentage points (positive = the ERP looks better than reality)
- setup_vs_standard: real setup time as a multiple of the standard
- stops: stops the machine data shows; stop_minutes: their minutes (baseline = usual day)
- stops_not_logged: of those stops, how many no operator wrote down
- logged_stops, logged_minutes: what the operators' downtime log holds
- scrap_pct: scrap rate on the plant day (baseline = rate over the baseline window)
- standard_errors: how far the rate is from its baseline, in standard errors (negative = below)
- anomaly:* : an alert; value is what tripped the rule, threshold what it beat; status is New or since when it has fired

## Verdicts

Record a finding for each statement that is:

- **wrong**: a fact contradicts it. A number attached to the wrong machine,
  shift, work center, or metric (even if the number is cited: a block citing
  several facts can put one fact's number on another's subject); the wrong
  direction (above vs below baseline, up vs down, more vs fewer); a comparison
  the values contradict ("higher than usual" when value < baseline; "all
  logged" when stops_not_logged > 0); a wrong day or date.
- **invented**: a claim no fact supports: a limit, range, or threshold no fact
  gives; "all machines normal" / "nothing unusual" / "within normal range"
  when the facts don't say so (check every such sweeping claim against every
  relevant fact); a trend across days; anything about the future.
- **cause**: a cause, explanation, advice, or action the facts don't give
  ("likely nozzle wear", "driven by long setups", "worth investigating",
  "operators should log stops"). Describing two facts side by side is fine;
  saying one explains the other is a cause.
- **status**: an alert called new when its status says it has been open, or
  the reverse, or a wrong "since" date.

Also record, as **minor** (not misleading), things worth a note but not a
misreading: an alert mentioned without its status, the plant day called
"today", awkward or garbled wording that is still true, a section missing.

Do NOT record: rounding that stays true; missing or misplaced citations; numbers
in headings; length, layout, or listing every fact; a fact simply left out.

Check every comparative and sweeping word: above, below, higher, lower, more,
fewer, usual, normal, typical, worse, better, improved, all, none, every, only,
most, no machine, still, again.

## Output

Write one JSON file (path given in your task): a list with one object per
brief, in the order given:

```json
{"model": "nova-v2", "morning": "2026-07-06", "log": "<the packet's log line>",
 "findings": [
   {"quote": "<exact substring of the brief as delivered>",
    "verdict": "wrong|invented|cause|status|minor",
    "fact": "<the fact ID(s) and values it contradicts, or 'none' for invented/cause>",
    "note": "<one sentence: what the manager would believe, and what the facts say>"}
 ]}
```

`quote` is checked by machine: copy it character for character from the
"brief as delivered" section, Markdown asterisks, brackets, and dashes
included, and keep it to the sentence or bullet that says the thing (no more
than about 300 characters; a sub-part of a long bullet is fine). A brief with
nothing to record has `"findings": []`. Read every brief whole, every line.
Be strict about what the facts say and fair to what the brief says: if a
reading of the sentence is true, it is not wrong.
