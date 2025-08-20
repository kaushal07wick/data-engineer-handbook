# Week 5 — Data Pipeline Maintenance

_Last updated: 2025-08-20 (Timezone: Asia/Kolkata)_

## Team & Scope

We are a team of **4 data engineers** responsible for **5 pipelines** across Profit, Growth, and Engagement KPIs.

**Team**
- **Alex Mehta (AM)**
- **Priya Rao (PR)**
- **Chen Wu (CW)**
- **Samir Kapoor (SK)**

**Pipelines**
1. **Profit — Unit-level** (experiment-facing)
2. **Profit — Aggregate** (investor-facing)
3. **Growth — Aggregate** (investor-facing)
4. **Growth — Daily** (experiment-facing)
5. **Engagement — Aggregate** (investor-facing)

---

## Ownership (Primary & Secondary)

| Pipeline | Domain | Type | Primary | Secondary |
|---|---|---|---|---|
| Profit — Unit-level | Profit | Experiment | **PR** | **CW** |
| Profit — Aggregate | Profit | Investor | **AM** | **PR** |
| Growth — Aggregate | Growth | Investor | **CW** | **SK** |
| Growth — Daily | Growth | Experiment | **SK** | **AM** |
| Engagement — Aggregate | Engagement | Investor | **PR** | **AM** |

> Rationale: Each engineer owns at least one investor-facing pipeline and one experiment-facing pipeline; secondaries avoid pairing the same two people repeatedly to improve bus factor and coverage.

---

## On-call Policy & Schedule

### Policy (Summary)
- **Coverage window:** 24×7 for investor-facing pipelines; business-hours (09:00–19:00 IST) for experiment pipelines with best-effort after-hours.
- **Rotation length:** 1-week blocks (Mon 09:00 IST → Mon 09:00 IST).
- **Roles per week:** _Primary on-call_ + _Backup on-call_. Primary pages first; if no ACK in 10 min, page escalates to Backup; if no ACK in 15 min, page to **Incident Commander (IC)** (the secondary owner of the impacted pipeline if not already paged).
- **Handover:** 30-minute synchronous handoff on Mondays 09:00–09:30 IST with review of open incidents, suppressions, and pending RCAs.
- **Holidays:** If a scheduled on-caller is on a **company-recognized holiday** or approved PTO, they must pre-arrange a voluntary swap. If a critical national holiday falls within a rotation (e.g., Republic Day, Holi, Independence Day, Diwali), the **Backup** becomes **Primary** for that day, and the normally scheduled **Primary** becomes **Backup**. Swaps are logged in the schedule table.
- **Paging rules:** Investor-facing SLOs page immediately on breach of latency/freshness/error-rate; experiment pipelines notify Slack during business hours unless configured as hard dependency to investor pipelines.
- **RCA/SLOs:** RCAs due within 5 business days for investor-facing pipeline Sev-1/Sev-2 incidents. SLOs: _Freshness_ ≤ 15 min (investor daily), _Completeness_ ≥ 99.5%, _Accuracy drift_ ≤ 0.5pp from backfill truth.

### Rotation (8-week example, IST)
| Week # | Dates (Mon→Mon) | Primary | Backup |
|---|---|---|---|
| 1 | 2025-08-25 → 2025-09-01 | AM | PR |
| 2 | 2025-09-01 → 2025-09-08 | PR | CW |
| 3 | 2025-09-08 → 2025-09-15 | CW | SK |
| 4 | 2025-09-15 → 2025-09-22 | SK | AM |
| 5 | 2025-09-22 → 2025-09-29 | AM | CW |
| 6 | 2025-09-29 → 2025-10-06 | PR | SK |
| 7 | 2025-10-06 → 2025-10-13 | CW | AM |
| 8 | 2025-10-13 → 2025-10-20 | SK | PR |

**Holiday Coverage Examples**
- **Holi (example):** If Holi occurs on Wed of Week 2 when **PR** is Primary and **CW** is Backup, roles swap for Wed only; table annotation: `2025-xx-xx (Holi): CW⇄PR`.
- **Diwali (example):** If Diwali spans Fri of Week 8, **Backup (PR)** becomes **Primary** that day; **SK** is Backup.

**Escalation Ladder (Sev-1)**
1. Primary On-call (pager)
2. Backup On-call (pager)
3. Incident Commander (secondary owner of impacted pipeline)
4. Data Platform Lead (whoever is not Primary/Backup that week)

---

## Runbooks — Investor-Facing Pipelines

> These runbooks outline _what could go wrong_, blast radius, and **diagnostic steps** only. This is an imagination exercise; no prescriptive fixes provided.

### A. Profit — Aggregate (Investor)
**Purpose:** Daily aggregate profit delivered to Investor Warehouse and BI dashboards by 07:00 IST.
**Upstream Inputs:** Orders fact, Payments fact, Refunds, Discounts, COGS lookup, Experiment flags (excluded).
**Downstream:** Investor BI dashboard, Finance close, CFO weekly brief.

**Potential Failure Modes**
- **Data Freshness Breach:** Late upstream ingestion of Payments/Refunds; scheduler lag; cluster capacity starvation.
- **Completeness Drop:** Missed partitions (e.g., late-arriving orders), upstream dedupe misconfigured, CDC snapshot gap.
- **Accuracy Drift:** Currency FX rate misalignment, COGS lookup version mismatch, rounding changes, time-zone boundary shifts (UTC vs IST cutoff).
- **Schema Changes:** New columns in Orders/Payments; type widening (int→decimal); nullability changes.
- **Business Logic Drift:** Profit definition update not versioned; experiment traffic accidentally included.
- **Access/Secrets:** Expired warehouse credentials; revoked service account; KMS permission changes.
- **Infra/Orchestration:** DAG paused; cron misfire; resource quotas; malformed container image.
- **BI Publish Failure:** Table published but semantic layer not refreshed; dashboard extracts stale.

**Diagnostics Checklist**
1. **Is it late?** Check orchestrator run status, queue depth, last success timestamp, and SLA timer.
2. **Upstream health:** Validate latest partition timestamps for Orders/Payments/Refunds; compare record counts to 7/14/28-day baselines.
3. **Completeness:** Run partition diff for `order_date` vs `payment_date`; check late-arrival policy windows.
4. **Accuracy spot-checks:** Recompute profit with previous COGS/FX versions; compare variance; sample 100 orders.
5. **Schema drift:** Diff current vs last schema in catalog; validate contracts; check transformation compile logs.
6. **Access/secrets:** Validate token expiry, service-account role bindings, KMS decrypt.
7. **Downstream publish:** Confirm semantic layer refresh timestamp; cache/ETL publish logs.
8. **Comms:** Post status in `#investor-kpis`; open incident if SLA at risk (>30 min).

---

### B. Growth — Aggregate (Investor)
**Purpose:** Daily active users (DAU/WAU/MAU) and new-user adds; delivery by 06:30 IST.
**Upstream Inputs:** Auth events, App/SDK events, Device table, Geo/IP enrichment, Bot filter rules.
**Downstream:** Investor growth dashboard; Earnings package.

**Potential Failure Modes**
- **Event Loss/Delay:** Kafka backlog, mobile SDK offline buffers, ingestion throttling.
- **Bot/Spam Filter Drift:** Over/under-filtering from rule changes; IP/geofence list update failures.
- **Identity Resolution Issues:** Device resets, cookie churn, ID-graph merge rules changed.
- **Timezone/Windowing:** Day-boundary mismatch (UTC vs local); daylight saving in non-IST markets.
- **Schema Changes:** Event version bump; nulls in required fields; PII hashing changes.
- **Infra:** Stream job checkpoint corruption; watermark stuck; backfill overwhelms cluster.

**Diagnostics Checklist**
1. **Stream lag:** Check consumer group lag; compare to normal p95.
2. **Ingestion rate:** Events/min vs 7-day baseline; SDK error logs.
3. **Filter health:** Rule version diff; bot-hit rate vs baseline; sample quarantined events.
4. **Identity graph:** Unique users vs devices ratio; recent merge/split rate anomalies.
5. **Windowing:** Verify sessionization/window configs; boundary timestamps.
6. **Schema/PII:** Contract diff; required fields null-rate; hash salt version.
7. **Checkpoint/watermark:** Inspect latest checkpoint; watermark timestamps; consider replay window.
8. **Comms:** Notify `#investor-kpis` if SLA risk.

---

### C. Engagement — Aggregate (Investor)
**Purpose:** Engagement score (sessions/user, time-in-app, retention) by 07:00 IST.
**Upstream Inputs:** App events, Sessionization job output, Content metadata, A/B exclusions.
**Downstream:** Investor engagement dashboard; IR materials.

**Potential Failure Modes**
- **Sessionization Faults:** Wrong idle-timeout; duplicate session IDs; late events not reattached.
- **Metric Definition Drift:** Engagement-scoring weights changed; experiment traffic leakage.
- **Content Join Failures:** Missing content IDs; metadata ETL late; null explosion after joins.
- **Skew/Hot Keys:** Popular content causing executor OOM; shuffle retries.
- **Sampling/Extraction Errors:** Incorrect percentile calculations; window misalignment.
- **Publishing/Cache:** Cache TTL expired without refresh; BI extraction timeout.

**Diagnostics Checklist**
1. **Session job health:** Last run status; idle-timeout config; duplicate key rate.
2. **Definition version:** Confirm metric version in catalog; compare output deltas to prior version.
3. **Join completeness:** Null rate after joins; orphaned content IDs; left vs inner join counts.
4. **Skew:** Top-key distribution; executor retries/OOM; skew mitigation flags.
5. **Sampling:** Verify window params; recompute small sample; percentile sanity checks.
6. **Publishing:** Check semantic layer timestamp; cache status; dashboard query errors.
7. **Comms:** Update `#investor-kpis` if at-risk.

---

## Non-Investor Pipelines (for completeness)

### Profit — Unit-level (Experiment)
- **Purpose:** Provide per-order profit features for experiment analysis within 15 min of order close.
- **Common Risks:** Late payments/refunds, version skew of COGS/FX, feature-store TTL misconfig, data leakage between control/treatment, schema changes.

### Growth — Daily (Experiment)
- **Purpose:** Daily cut of growth metrics for experiment scorecards by 10:00 IST.
- **Common Risks:** Event lag, identity graph changes, filter drift, day boundary mismatches, mismatched experiment exclusion lists.

---

## Operational Standards
- **Dashboards:** _Ops Overview_, _Freshness & Lag_, _Error Budget Burn_, _Schema Contracts_.
- **Run Artefacts:** Each DAG writes run metadata (run_id, started_at, ended_at, records_out, checksum) to the **Ops Warehouse**.
- **Contracts:** All upstreams enforce schema contracts with fail-closed for investor pipelines.
- **Versioning:** Metric definitions and reference data (COGS, FX, bot lists) are versioned and recorded in output tables.
- **Backfills:** Investor pipelines require explicit approval + read-only guardrails; experiment pipelines can auto-backfill within a 7-day window.
- **Access:** Service accounts rotated every 90 days; least-privilege maintained via IAM.
- **Post-Incident:** Blameless RCAs with action items and owners; learning captured in runbooks.

---

## Contacts
- **#investor-kpis** (Slack) — status & incident updates
- **#data-platform** (Slack) — platform issues
- **Email DL:** data-eng-oncall@company.example
