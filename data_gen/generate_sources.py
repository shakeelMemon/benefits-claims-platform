"""Generate synthetic source data for one business day of a group benefits operation.

Sources (what the Canadian Operations systems would send):
  landing/policy_admin/members_<date>.csv   daily member extract from the policy admin system (legacy SSIS feed)
  landing/reference/plans.csv               group plans
  landing/reference/providers.csv           health providers
  landing/finance/premiums_<date>.csv       monthly premium billed per employer group
  stream/claim_events_<date>.jsonl          claim lifecycle events (what goes onto the Kafka topic)

Day 1 builds the book of business. Day 2 changes some members (province, plan, coverage tier),
adds 40 new members, and sends more claim events, including late events for day-1 claims.

Planted data problems (the pipeline must catch every one):
  - duplicate events (Kafka delivers at least once)
  - one malformed JSON line
  - a claim for a member that doesn't exist
  - a negative claimed amount
  - a claim paid for more than was claimed
  - an event with an unknown event type
  - events arriving out of order

All data is fake. Names, SINs and postal codes are made up.
"""

import argparse
import csv
import json
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DAYS = {1: date(2026, 10, 20), 2: date(2026, 10, 21)}

PROVINCES = ["ON", "ON", "ON", "QC", "QC", "BC", "AB", "MB", "NS"]
FSA = {"ON": ["M5V", "L3R", "K1A", "N2L"], "QC": ["H3B", "G1R"], "BC": ["V6B", "V8W"],
       "AB": ["T2P", "T5J"], "MB": ["R3C"], "NS": ["B3H"]}
FIRST = ["Aisha", "Ben", "Chloe", "Daniel", "Elena", "Farah", "Gabriel", "Hana", "Imran", "Julia",
         "Kevin", "Leila", "Marc", "Nadia", "Omar", "Priya", "Quinn", "Rosa", "Sami", "Tara"]
LAST = ["Ahmed", "Brown", "Chen", "Dubois", "Evans", "Fernandes", "Gagnon", "Hussain", "Ivanova",
        "Johnson", "Khan", "Lee", "Martin", "Nguyen", "Patel", "Roy", "Singh", "Tremblay", "Wong"]
GROUPS = [("G100", "Maple Logistics Inc.", "Transportation"),
          ("G200", "Northern Lights Software", "Technology"),
          ("G300", "Lakeshore Hospital Services", "Healthcare"),
          ("G400", "Prairie Grain Co-op", "Agriculture")]
PLANS = [("PL-G100-STD", "G100", "Standard Health & Dental"),
         ("PL-G100-ENH", "G100", "Enhanced Health & Dental"),
         ("PL-G200-STD", "G200", "Standard Health & Dental"),
         ("PL-G200-ENH", "G200", "Enhanced Health & Dental"),
         ("PL-G300-STD", "G300", "Standard Health & Dental"),
         ("PL-G400-STD", "G400", "Standard Health & Dental")]
PROVIDER_TYPES = {"DRUG": "PHARMACY", "DENTAL": "DENTIST", "PARAMEDICAL": "PHYSIOTHERAPIST",
                  "VISION": "OPTOMETRIST", "HEALTH": "CLINIC"}
BENEFIT_MIX = ["DRUG"] * 5 + ["DENTAL"] * 3 + ["PARAMEDICAL"] * 2 + ["VISION", "HEALTH"]
AMOUNT = {"DRUG": (12, 180), "DENTAL": (90, 900), "PARAMEDICAL": (60, 140), "VISION": (150, 450),
          "HEALTH": (40, 600)}
CHANNELS = ["MOBILE_APP"] * 4 + ["PROVIDER_EDI"] * 5 + ["PAPER"]
DENIAL_REASONS = ["ANNUAL_MAX_REACHED", "NOT_COVERED", "MISSING_RECEIPT", "DUPLICATE_SUBMISSION"]


def ts(d: date, rng: random.Random, hour_from=7, hour_to=22) -> datetime:
    return datetime(d.year, d.month, d.day, rng.randint(hour_from, hour_to - 1),
                    rng.randint(0, 59), rng.randint(0, 59), tzinfo=timezone.utc)


def iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def fake_sin(rng):
    return f"{rng.randint(100, 999)}-{rng.randint(100, 999)}-{rng.randint(100, 999)}"


def build_members(rng: random.Random, n=3000):
    members = []
    for i in range(1, n + 1):
        plan_code, group_id, _ = rng.choice(PLANS)
        prov = rng.choice(PROVINCES)
        members.append({
            "member_id": f"M{i:05d}",
            "group_id": group_id,
            "first_name": rng.choice(FIRST),
            "last_name": rng.choice(LAST),
            "sin": fake_sin(rng),
            "date_of_birth": (date(1960, 1, 1) + timedelta(days=rng.randint(0, 15000))).isoformat(),
            "province": prov,
            "postal_code": f"{rng.choice(FSA[prov])} {rng.randint(1, 9)}X{rng.randint(1, 9)}",
            "plan_code": plan_code,
            "coverage_tier": rng.choice(["SINGLE", "COUPLE", "FAMILY", "FAMILY"]),
            "member_status": "ACTIVE",
            "coverage_effective_date": (date(2023, 1, 1) + timedelta(days=rng.randint(0, 900))).isoformat(),
        })
    return members


def day2_changes(rng: random.Random, members):
    """Province moves, plan upgrades, tier changes, one termination, and 40 new hires."""
    changed = [dict(m) for m in members]
    for m in rng.sample(changed, 150):
        change = rng.choice(["move", "plan", "tier"])
        if change == "move":
            m["province"] = rng.choice([p for p in set(PROVINCES) if p != m["province"]])
            m["postal_code"] = f"{rng.choice(FSA[m['province']])} {rng.randint(1, 9)}X{rng.randint(1, 9)}"
        elif change == "plan":
            options = [p for p in PLANS if p[1] == m["group_id"] and p[0] != m["plan_code"]]
            if options:
                m["plan_code"] = options[0][0]
        else:
            m["coverage_tier"] = rng.choice([t for t in ["SINGLE", "COUPLE", "FAMILY"] if t != m["coverage_tier"]])
    rng.choice(changed)["member_status"] = "TERMINATED"
    start = len(changed) + 1
    for i in range(start, start + 40):
        plan_code, group_id, _ = rng.choice(PLANS)
        prov = rng.choice(PROVINCES)
        changed.append({
            "member_id": f"M{i:05d}", "group_id": group_id, "first_name": rng.choice(FIRST),
            "last_name": rng.choice(LAST), "sin": fake_sin(rng),
            "date_of_birth": (date(1965, 1, 1) + timedelta(days=rng.randint(0, 14000))).isoformat(),
            "province": prov, "postal_code": f"{rng.choice(FSA[prov])} {rng.randint(1, 9)}X{rng.randint(1, 9)}",
            "plan_code": plan_code, "coverage_tier": "SINGLE", "member_status": "ACTIVE",
            "coverage_effective_date": "2026-09-01",            # hired last month, first extract today
        })
    return changed


def build_providers(rng):
    providers, i = [], 1
    for benefit, ptype in PROVIDER_TYPES.items():
        for _ in range(8 if benefit == "DRUG" else 5):
            prov = rng.choice(PROVINCES)
            providers.append({"provider_id": f"P{i:04d}", "provider_name": f"{rng.choice(LAST)} {ptype.title()} #{i}",
                              "provider_type": ptype, "benefit_type": benefit, "province": prov})
            i += 1
    return providers


def claim_lifecycle(rng, claim_id, member, provider, day: date, split_days: bool):
    """Return events for one claim. If split_days, the final event lands on the next day."""
    benefit = provider["benefit_type"]
    lo, hi = AMOUNT[benefit]
    claimed = round(rng.uniform(lo, hi), 2)
    submitted = ts(day, rng, 7, 18)
    base = {"claim_id": claim_id, "member_id": member["member_id"], "plan_code": member["plan_code"],
            "provider_id": provider["provider_id"], "benefit_type": benefit,
            "service_date": (submitted.date() - timedelta(days=rng.randint(0, 20))).isoformat(),
            "channel": rng.choice(CHANNELS), "currency": "CAD"}
    events = [dict(base, event_type="SUBMITTED", event_ts=iso(submitted), amount_claimed=claimed)]
    adjudicated = submitted + timedelta(minutes=rng.randint(1, 600) if events[0]["channel"] != "PAPER"
                                        else rng.randint(600, 1200))
    denied = rng.random() < 0.08
    if denied:
        events.append(dict(base, event_type="DENIED", event_ts=iso(adjudicated), amount_claimed=claimed,
                           denial_reason=rng.choice(DENIAL_REASONS)))
        return events, None
    coinsurance = 0.8 if benefit in ("DENTAL", "PARAMEDICAL") else 0.9 if benefit == "DRUG" else 1.0
    eligible = round(claimed * rng.choice([1.0, 1.0, 1.0, 0.85]), 2)
    paid = round(eligible * coinsurance, 2)
    events.append(dict(base, event_type="ADJUDICATED", event_ts=iso(adjudicated), amount_claimed=claimed,
                       amount_eligible=eligible))
    paid_ts = adjudicated + timedelta(hours=rng.randint(1, 30) if split_days else rng.randint(1, 3))
    paid_event = dict(base, event_type="PAID", event_ts=iso(paid_ts), amount_claimed=claimed,
                      amount_eligible=eligible, amount_paid=paid)
    if split_days:
        return events, paid_event       # payment arrives in tomorrow's stream
    events.append(paid_event)
    return events, None


def with_ids(events):
    for e in events:
        e["event_id"] = str(uuid.UUID(int=random.getrandbits(128)))
    return events


def write_csv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", type=int, choices=[1, 2], required=True)
    ap.add_argument("--out", default=str(ROOT / "local"))
    args = ap.parse_args()

    random.seed(1000 + args.day)          # event ids: repeatable, different per day
    rng = random.Random(42)
    out = Path(args.out)
    members = build_members(rng)
    providers = build_providers(rng)
    carried = []                          # day-1 payments delivered on day 2
    day1_events = []
    for n in range(1, 701):
        m, p = rng.choice(members), rng.choice(providers)
        ev, late = claim_lifecycle(rng, f"C1{n:06d}", m, p, DAYS[1], split_days=rng.random() < 0.15)
        day1_events += ev
        if late:
            carried.append(late)

    if args.day == 1:
        events, extract, run_day = day1_events, members, DAYS[1]
        events += [dict(events[10]), dict(events[11]), dict(events[57])]            # duplicates (same event_id later)
        bad = dict(events[30], claim_id="C1999901", member_id="M99999", event_type="SUBMITTED")
        neg = dict(events[40], claim_id="C1999902", event_type="SUBMITTED", amount_claimed=-25.0)
        events += [bad, neg]
    else:
        rng2 = random.Random(7)
        extract = day2_changes(rng2, members)
        events = list(carried)
        for n in range(1, 801):
            m, p = rng2.choice(extract), rng2.choice(providers)
            if m["member_status"] != "ACTIVE":
                continue
            ev, _ = claim_lifecycle(rng2, f"C2{n:06d}", m, p, DAYS[2], split_days=rng2.random() < 0.1)
            events += ev                  # ~10% are still waiting for payment at day end
        over = [e for e in events if e["event_type"] == "PAID"][5]
        over["amount_paid"] = round(over["amount_claimed"] * 1.5, 2)                 # paid more than claimed
        events.append(dict(events[3], event_type="REOPENED"))                        # unknown event type
        run_day = DAYS[2]

    with_ids(events)
    if args.day == 1:
        n = len(events)
        for i, src in zip(range(n - 5, n - 2), (10, 11, 57)):
            events[i]["event_id"] = events[src]["event_id"]                          # true duplicates
    rng.shuffle(events)                                                              # out of order

    stamp = run_day.strftime("%Y%m%d")
    write_csv(out / "landing" / "policy_admin" / f"members_{stamp}.csv", extract)
    write_csv(out / "landing" / "reference" / "plans.csv",
              [{"plan_code": c, "group_id": g, "plan_name": n,
                "group_name": next(x[1] for x in GROUPS if x[0] == g),
                "industry": next(x[2] for x in GROUPS if x[0] == g)} for c, g, n in PLANS])
    write_csv(out / "landing" / "reference" / "providers.csv", providers)
    month_start = run_day.replace(day=1)
    write_csv(out / "landing" / "finance" / f"premiums_{stamp}.csv",
              [{"group_id": g, "billing_month": month_start.isoformat(),
                "premium_cad": round(sum(1 for m in extract if m["group_id"] == g) * rng.uniform(310, 360), 2)}
               for g, _, _ in GROUPS])
    stream = out / "stream" / "incoming"
    stream.mkdir(parents=True, exist_ok=True)
    with open(stream / f"claim_events_{stamp}.jsonl", "w", encoding="utf-8") as f:
        for i, e in enumerate(events):
            f.write(json.dumps(e) + "\n")
            if args.day == 1 and i == 100:
                f.write('{"event_id": "broken-1", "claim_id": "C1000001", "event_type": "PAID", \n')  # malformed
    print(f"day {args.day} ({run_day}): {len(extract)} members, {len(providers)} providers, "
          f"{len(events)} claim events -> {out}")


if __name__ == "__main__":
    main()
