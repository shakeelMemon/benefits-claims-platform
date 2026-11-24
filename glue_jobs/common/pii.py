"""Personal information controls applied on the way into silver.

Rule: analytics never needs a member's name, SIN, exact birth date or full postal code,
so silver keeps none of them (data minimization). What stays is enough for analysis:
an age band, the postal code's first three characters (FSA) and the province.
Bronze is the only zone with raw PII; in AWS it is encrypted with KMS and only the
pipeline role can read it.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

PII_COLUMNS = ["first_name", "last_name", "sin", "date_of_birth", "postal_code"]


def age_band(dob: Column, as_of: Column) -> Column:
    age = F.floor(F.months_between(as_of, dob) / 12)
    return (F.when(age < 30, "18-29")
             .when(age < 40, "30-39")
             .when(age < 50, "40-49")
             .when(age < 60, "50-59")
             .otherwise("60+"))


def postal_fsa(postal_code: Column) -> Column:
    return F.upper(F.substring(F.regexp_replace(postal_code, r"\s", ""), 1, 3))


def drop_pii(df):
    return df.drop(*[c for c in PII_COLUMNS if c in df.columns])
