-- Business questions answered from the gold layer.
-- In AWS: run in Athena (workgroup claims-analytics, database claims_gold) after the crawler has run.
-- Locally: python sql/query_local.py runs the same queries with DuckDB over the Parquet files.

-- 1. How much did we pay this month, by benefit type?
select benefit_type,
       sum(claims)        as claims,
       sum(amount_paid)   as paid_cad,
       round(sum(claims_denied) * 1.0 / sum(claims), 3) as denial_rate
from mart_claims_monthly
group by benefit_type
order by paid_cad desc;

-- 2. Which employer groups are running hot? (month-to-date loss ratio = claims paid / premium)
select group_name, industry, billing_month, premium_cad, claims_paid_cad, loss_ratio
from mart_loss_ratio
order by loss_ratio desc;

-- 3. How fast do we close claims, by channel? (operations service level)
select channel, claims_closed, avg_turnaround_hours, median_turnaround_hours, pct_closed_within_24h
from mart_turnaround_by_channel
order by avg_turnaround_hours;

-- 4. Why are claims denied?
select denial_reason, count(*) as claims, sum(amount_claimed) as amount_claimed
from fact_claim
where claim_status = 'DENIED'
group by denial_reason
order by claims desc;

-- 5. Claims still open (adjudicated or submitted, not yet paid or denied)
select claim_status, count(*) as claims, sum(amount_eligible) as pending_liability_cad
from fact_claim
where claim_status in ('SUBMITTED', 'ADJUDICATED')
group by claim_status;

-- 6. SCD Type 2 in action: the same member's claims, before and after a plan or address change
with claims_by_version as (
    select f.member_id, f.claim_id, f.service_date, d.version, d.plan_code, d.coverage_tier, d.province,
           d.valid_from, d.valid_to, d.is_current
    from fact_claim f
    join dim_member d on d.member_sk = f.member_sk
),
changed_members as (
    select member_id from claims_by_version group by member_id having count(distinct version) > 1
)
select * from claims_by_version
where member_id in (select member_id from changed_members)
order by member_id, service_date
limit 20;

-- 7. Paid amount by province and age band (no names or SINs anywhere in gold)
select d.province, d.age_band, count(*) as claims, sum(f.amount_paid) as paid_cad
from fact_claim f
join dim_member d on d.member_sk = f.member_sk
group by d.province, d.age_band
order by paid_cad desc
limit 15;

-- 8. Data-quality history: what failed, and when?
-- (dq results live in lake/dq/results; add a crawler target or query them locally)
