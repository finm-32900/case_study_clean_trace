# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Data Sources Overview
#
# Quick look at each raw dataset pulled by the pipeline.
# Fourteen datasets are currently collected:
#
# 1. **Fama-French SIC Codes** — industry classification ranges (Dartmouth/Kenneth French)
# 2. **FISD Amount Outstanding History** — historical par amount changes (WRDS)
# 3. **FISD Issue** — bond-level reference data (WRDS)
# 4. **FISD Issuer** — issuer-level reference data (WRDS)
# 5. **FISD Ratings (Moody's)** — Moody's bond ratings history (WRDS)
# 6. **FISD Ratings (S&P)** — S&P bond ratings history (WRDS)
# 7. **FISD Redemption** — callable/redemption flags (WRDS)
# 8. **Liu-Wu Treasury Yields** — zero-coupon yield curve (Google Sheets)
# 9. **OSBAP Corporate Bond Returns** — monthly bond returns (OpenBondAssetPricing.com)
# 10. **OSBAP Linker** — equity identifier linker (OpenBondAssetPricing.com)
# 11. **OSBAP Treasury Bond Returns** — treasury returns (OpenBondAssetPricing.com)
# 12. **TRACE 144A** — Rule 144A private placement trades (WRDS)
# 13. **TRACE Enhanced** — trade-level transaction data (WRDS)
# 14. **TRACE Standard** — trade-level transaction data (WRDS)

# %%
from pathlib import Path

import polars as pl

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
PULL_DIR = DATA_DIR / "pulled"

# %% [markdown]
# ---
# ## 1. Fama-French SIC Codes — `pulled/ff17_sic_ranges.parquet` + `pulled/ff30_sic_ranges.parquet`
#
# Source script: `pull_fama_french_sic.py` | Source: Kenneth French Data Library (Dartmouth)
#
# Industry-to-SIC-code range mappings from the Fama-French 17- and 30-industry
# classification systems. Each row maps an industry number and name to a
# contiguous SIC code range. Used by Stage 1 for industry classification.
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `ind_num` | Industry number in the FF classification |
# | `ind_name` | Short industry name |
# | `sic_start` | Start of SIC code range |
# | `sic_end` | End of SIC code range |

# %%
ff17 = pl.scan_parquet(PULL_DIR / "ff17_sic_ranges.parquet")
n_rows_ff17 = ff17.select(pl.len()).collect().item()
cols_ff17 = ff17.collect_schema().names()
print(f"FF17 — Rows: {n_rows_ff17:,}  |  Columns: {len(cols_ff17)}")
print(f"Column names: {cols_ff17}")

# %% [markdown]
# ### Example rows — FF17

# %%
ff17.head(5).collect()

# %%
ff30 = pl.scan_parquet(PULL_DIR / "ff30_sic_ranges.parquet")
n_rows_ff30 = ff30.select(pl.len()).collect().item()
cols_ff30 = ff30.collect_schema().names()
print(f"FF30 — Rows: {n_rows_ff30:,}  |  Columns: {len(cols_ff30)}")
print(f"Column names: {cols_ff30}")

# %% [markdown]
# ### Example rows — FF30

# %%
ff30.head(5).collect()

# %% [markdown]
# ---
# ## 2. FISD Amount Outstanding History — `pulled/fisd_amt_out_hist.parquet`
#
# Source script: `pull_fisd.py` | WRDS table: `fisd.fisd_amt_out_hist`
#
# Historical changes in par amount outstanding for each bond issue.
# Records actions such as increases, decreases, and maturities that
# change the total amount of a bond outstanding over time.

# %%
amt_out = pl.scan_parquet(PULL_DIR / "fisd_amt_out_hist.parquet")
n_rows_amt = amt_out.select(pl.len()).collect().item()
cols_amt = amt_out.collect_schema().names()
print(f"Rows: {n_rows_amt:,}  |  Columns: {len(cols_amt)}")
print(f"Column names: {cols_amt}")

# %% [markdown]
# ### Example rows

# %%
amt_out.head(5).collect()

# %% [markdown]
# ---
# ## 3. FISD Issue Data — `pulled/fisd_issue.parquet`
#
# Source script: `pull_fisd.py` | WRDS table: `fisd.fisd_mergedissue`
#
# One row per fixed-income security (bond issue) in the Mergent FISD database.
# Contains bond characteristics such as coupon, maturity, and offering details.
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `complete_cusip` | 9-character CUSIP identifier |
# | `issue_name` | Descriptive name of the bond issue |
# | `issuer_id` | Foreign key to the issuer table |
# | `coupon` | Annual coupon rate (%) |
# | `coupon_type` | Coupon type (F=fixed, V=variable, etc.) |
# | `maturity` | Maturity date |
# | `offering_date` | Original offering date |
# | `bond_type` | Bond type code (CDEB, CMTN, etc.) |
# | `offering_amt` | Offering amount ($ millions) |
# | `principal_amt` | Par value per bond (typically 1000) |

# %%
fisd_issue = pl.scan_parquet(PULL_DIR / "fisd_issue.parquet")
n_rows = fisd_issue.select(pl.len()).collect().item()
cols = fisd_issue.collect_schema().names()
print(f"Rows: {n_rows:,}  |  Columns: {len(cols)}")
print(f"Column names: {cols}")

# %% [markdown]
# ### Example rows

# %%
fisd_issue.head(5).collect()

# %% [markdown]
# ### Date range

# %%
fisd_issue.select(
    pl.col("offering_date").min().alias("earliest_offering"),
    pl.col("offering_date").max().alias("latest_offering"),
    pl.col("maturity").min().alias("earliest_maturity"),
    pl.col("maturity").max().alias("latest_maturity"),
).collect()

# %% [markdown]
# ### Top 15 bond types

# %%
(
    fisd_issue.group_by("bond_type")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
    .head(15)
    .collect()
)

# %% [markdown]
# ### Bond types — bar chart

# %%
import matplotlib.pyplot as plt

bt = (
    fisd_issue.group_by("bond_type")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
    .head(15)
    .collect()
)

fig, ax = plt.subplots(figsize=(10, 3))
ax.bar(bt["bond_type"].to_list(), bt["n"].to_list(), color="steelblue", alpha=0.8)
ax.set_ylabel("Count")
ax.set_title("FISD — top 15 bond types")
ax.tick_params(axis="x", rotation=45)
fig.tight_layout()
plt.show()

# %% [markdown]
# ---
# ## 4. FISD Issuer Data — `pulled/fisd_issuer.parquet`
#
# Source script: `pull_fisd.py` | WRDS table: `fisd.fisd_mergedissuer`
#
# One row per bond issuer. Provides the country of domicile and SIC
# industry classification for each issuer. Linked to the issue table
# via `issuer_id`.
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `issuer_id` | Unique issuer identifier (joins to issue table) |
# | `country_domicile` | Country of domicile (ISO code) |
# | `sic_code` | Standard Industrial Classification code |

# %%
fisd_issuer = pl.scan_parquet(PULL_DIR / "fisd_issuer.parquet")
n_rows_issuer = fisd_issuer.select(pl.len()).collect().item()
cols_issuer = fisd_issuer.collect_schema().names()
print(f"Rows: {n_rows_issuer:,}  |  Columns: {len(cols_issuer)}")
print(f"Column names: {cols_issuer}")

# %% [markdown]
# ### Example rows

# %%
fisd_issuer.head(5).collect()

# %% [markdown]
# ### Top 10 countries of domicile

# %%
(
    fisd_issuer.group_by("country_domicile")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
    .head(10)
    .collect()
)

# %% [markdown]
# ---
# ## 5. FISD Ratings (Moody's) — `pulled/fisd_ratings_moodys.parquet`
#
# Source script: `pull_fisd.py` | WRDS table: `fisd.fisd_ratings` (filtered to `rating_type = 'MR'`)
#
# Historical Moody's credit ratings for each bond issue. One row per
# rating event (initial assignment, upgrade, downgrade).
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `issue_id` | FISD issue identifier |
# | `rating_date` | Date of the rating action |
# | `rating` | Moody's rating string (e.g. Aaa, Baa1) |

# %%
ratings_mdy = pl.scan_parquet(PULL_DIR / "fisd_ratings_moodys.parquet")
n_rows_mdy = ratings_mdy.select(pl.len()).collect().item()
cols_mdy = ratings_mdy.collect_schema().names()
print(f"Rows: {n_rows_mdy:,}  |  Columns: {len(cols_mdy)}")
print(f"Column names: {cols_mdy}")

# %% [markdown]
# ### Example rows

# %%
ratings_mdy.head(5).collect()

# %% [markdown]
# ### Date range

# %%
ratings_mdy.select(
    pl.col("rating_date").min().alias("earliest"),
    pl.col("rating_date").max().alias("latest"),
).collect()

# %% [markdown]
# ---
# ## 6. FISD Ratings (S&P) — `pulled/fisd_ratings_sp.parquet`
#
# Source script: `pull_fisd.py` | WRDS table: `fisd.fisd_ratings` (filtered to `rating_type = 'SPR'`)
#
# Historical S&P credit ratings for each bond issue. One row per
# rating event (initial assignment, upgrade, downgrade).
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `issue_id` | FISD issue identifier |
# | `rating_date` | Date of the rating action |
# | `rating` | S&P rating string (e.g. AAA, BBB+) |

# %%
ratings_sp = pl.scan_parquet(PULL_DIR / "fisd_ratings_sp.parquet")
n_rows_sp = ratings_sp.select(pl.len()).collect().item()
cols_sp = ratings_sp.collect_schema().names()
print(f"Rows: {n_rows_sp:,}  |  Columns: {len(cols_sp)}")
print(f"Column names: {cols_sp}")

# %% [markdown]
# ### Example rows

# %%
ratings_sp.head(5).collect()

# %% [markdown]
# ### Date range

# %%
ratings_sp.select(
    pl.col("rating_date").min().alias("earliest"),
    pl.col("rating_date").max().alias("latest"),
).collect()

# %% [markdown]
# ---
# ## 7. FISD Redemption — `pulled/fisd_redemption.parquet`
#
# Source script: `pull_fisd.py` | WRDS table: `fisd.fisd_mergedredemption`
#
# Callable/redeemable flags from the FISD redemption table. Indicates
# whether a bond issue is callable.
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `issue_id` | FISD issue identifier |
# | `callable` | Flag indicating whether the bond is callable |

# %%
redemp = pl.scan_parquet(PULL_DIR / "fisd_redemption.parquet")
n_rows_redemp = redemp.select(pl.len()).collect().item()
cols_redemp = redemp.collect_schema().names()
print(f"Rows: {n_rows_redemp:,}  |  Columns: {len(cols_redemp)}")
print(f"Column names: {cols_redemp}")

# %% [markdown]
# ### Example rows

# %%
redemp.head(5).collect()

# %% [markdown]
# ---
# ## 8. Liu-Wu Treasury Yields — `pulled/liu_wu_yields.parquet`
#
# Source script: `pull_liu_wu_yields.py` | Source: Liu-Wu (Google Sheets)
#
# Zero-coupon Treasury yield curve at selected maturities, daily-resampled
# with forward fill. Yields are expressed as decimals (e.g. 0.035 = 3.5%).
# Used by Stage 1 to compute credit spreads.
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `trd_exctn_dt` | Date |
# | `oneyr` | 1-year zero-coupon yield |
# | `twoyr` | 2-year zero-coupon yield |
# | `fiveyr` | 5-year zero-coupon yield |
# | `sevyr` | 7-year zero-coupon yield |
# | `tenyr` | 10-year zero-coupon yield |
# | `twentyr` | 20-year zero-coupon yield |
# | `thirtyr` | 30-year zero-coupon yield |

# %%
liu_wu = pl.scan_parquet(PULL_DIR / "liu_wu_yields.parquet")
n_rows_lw = liu_wu.select(pl.len()).collect().item()
cols_lw = liu_wu.collect_schema().names()
print(f"Rows: {n_rows_lw:,}  |  Columns: {len(cols_lw)}")
print(f"Column names: {cols_lw}")

# %% [markdown]
# ### Example rows

# %%
liu_wu.head(5).collect()

# %% [markdown]
# ### Date range

# %%
liu_wu.select(
    pl.col("trd_exctn_dt").min().alias("earliest"),
    pl.col("trd_exctn_dt").max().alias("latest"),
).collect()

# %% [markdown]
# ---
# ## 9. OSBAP Corporate Bond Returns — `pulled/corporate_bond_returns.parquet`
#
# Source script: `pull_open_source_bond.py`
#
# Monthly bond-level returns from the Open Source Bond Asset Pricing
# (OSBAP) project at [openbondassetpricing.com](https://openbondassetpricing.com).
# Used as an external benchmark to validate the pipeline's Stage 1 output
# (duration, convexity, credit spreads).
#
# **Key columns** (partial — see OSBAP documentation for full schema):
#
# | Column | Description |
# |---|---|
# | `cusip` | 9-character CUSIP |
# | `date` | Month-end date |
# | `bond_ret` | Total bond return |
# | `bond_prc` | Bond price |
# | `mod_dur` | Modified duration |
# | `convexity` | Convexity |
# | `credit_spread` | Credit spread (OAS) |

# %%
corp = pl.scan_parquet(PULL_DIR / "corporate_bond_returns.parquet")
n_rows_corp = corp.select(pl.len()).collect().item()
cols_corp = corp.collect_schema().names()
print(f"Rows: {n_rows_corp:,}  |  Columns: {len(cols_corp)}")
print(f"Column names: {cols_corp}")

# %% [markdown]
# ### Example rows

# %%
corp.head(5).collect()

# %% [markdown]
# ### Date range

# %%
date_cols_corp = [c for c in cols_corp if "date" in c.lower() or "dt" in c.lower()]
if date_cols_corp:
    corp.select(
        pl.col(date_cols_corp[0]).min().alias("earliest"),
        pl.col(date_cols_corp[0]).max().alias("latest"),
    ).collect()

# %% [markdown]
# ---
# ## 10. OSBAP Linker — `pulled/osbap_linker.parquet`
#
# Source script: `pull_open_source_bond.py` | Source: openbondassetpricing.com
#
# Equity identifier linker from the OSBAP project. Maps bond CUSIPs to
# equity identifiers (PERMNO, GVKEY, ticker) for cross-asset analysis.

# %%
linker = pl.scan_parquet(PULL_DIR / "osbap_linker.parquet")
n_rows_linker = linker.select(pl.len()).collect().item()
cols_linker = linker.collect_schema().names()
print(f"Rows: {n_rows_linker:,}  |  Columns: {len(cols_linker)}")
print(f"Column names: {cols_linker}")

# %% [markdown]
# ### Example rows

# %%
linker.head(5).collect()

# %% [markdown]
# ---
# ## 11. OSBAP Treasury Bond Returns — `pulled/treasury_bond_returns.parquet`
#
# Source script: `pull_open_source_bond.py`
#
# Treasury bond returns from the OSBAP project, used alongside corporate
# bond returns for spread calculations and benchmarking.

# %%
treas = pl.scan_parquet(PULL_DIR / "treasury_bond_returns.parquet")
n_rows_treas = treas.select(pl.len()).collect().item()
cols_treas = treas.collect_schema().names()
print(f"Rows: {n_rows_treas:,}  |  Columns: {len(cols_treas)}")
print(f"Column names: {cols_treas}")

# %% [markdown]
# ### Example rows

# %%
treas.head(5).collect()

# %% [markdown]
# ### Date range

# %%
date_cols_treas = [c for c in cols_treas if "date" in c.lower() or "dt" in c.lower()]
if date_cols_treas:
    treas.select(
        pl.col(date_cols_treas[0]).min().alias("earliest"),
        pl.col(date_cols_treas[0]).max().alias("latest"),
    ).collect()

# %% [markdown]
# ---
# ## 12. TRACE 144A — `pulled/trace_144a/`
#
# Source script: `pull_trace_144a.py` | WRDS table: `trace.trace_btds144a`
#
# Trade-level data for Rule 144A private placement bonds. These are
# bonds sold to qualified institutional buyers under SEC Rule 144A.
# Data starts July 2002. Same column schema as TRACE Standard.
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `cusip_id` | 9-character CUSIP bond identifier |
# | `trd_exctn_dt` | Trade execution date |
# | `trd_exctn_tm` | Trade execution time |
# | `rptd_pr` | Reported price (per $100 par) |
# | `ascii_rptd_vol_tx` | Reported volume (text field) |
# | `yld_pt` | Yield at time of trade |
# | `trc_st` | TRACE status |
# | `side` | Trade side |
# | `msg_seq_nb` | Message sequence number |

# %%
trace_144a_dir = PULL_DIR / "trace_144a"
trace_144a = pl.scan_parquet(
    trace_144a_dir / "**/*.parquet",
    hive_partitioning=True,
    allow_missing_columns=True,
)
n_rows_144a = trace_144a.select(pl.len()).collect().item()
cols_144a = trace_144a.collect_schema().names()
print(f"Rows: {n_rows_144a:,}  |  Columns: {len(cols_144a)}")
print(f"Column names: {cols_144a}")

# %% [markdown]
# ### Example rows

# %%
trace_144a.head(5).collect()

# %% [markdown]
# ### Date range

# %%
trace_144a.select(
    pl.col("trd_exctn_dt").min().alias("earliest"),
    pl.col("trd_exctn_dt").max().alias("latest"),
).collect()

# %% [markdown]
# ### Trades per month — TRACE 144A

# %%
import matplotlib.dates as mdates

t144a_monthly = (
    trace_144a.with_columns(pl.col("trd_exctn_dt").alias("date"))
    .group_by(pl.col("date").dt.truncate("1mo"))
    .agg(pl.len().alias("n_trades"))
    .sort("date")
    .collect()
)

fig, ax = plt.subplots(figsize=(10, 3))
ax.bar(
    t144a_monthly["date"].to_list(),
    t144a_monthly["n_trades"].to_list(),
    width=25,
    color="darkorange",
    alpha=0.8,
)
ax.xaxis.set_major_locator(mdates.YearLocator(2))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
ax.set_ylabel("Trades")
ax.set_title("TRACE 144A — trades per month")
fig.tight_layout()
plt.show()

# %% [markdown]
# ---
# ## 13. TRACE Enhanced — `pulled/trace_enhanced/`
#
# Source script: `pull_trace_enhanced.py` | WRDS table: `trace.trace_enhanced`
#
# Trade-level corporate bond transaction data from FINRA's TRACE system
# (Enhanced version). Data starts July 2002 and is stored as a
# hive-partitioned data lake (`trace_enhanced/year=YYYY/month=MM/data.parquet`).
# Each row is one reported trade with price, volume, and counterparty details.
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `cusip_id` | 9-character CUSIP bond identifier |
# | `trd_exctn_dt` | Trade execution date |
# | `trd_exctn_tm` | Trade execution time |
# | `rptd_pr` | Reported price (per $100 par) |
# | `entrd_vol_qt` | Entered volume (par value) |
# | `yld_pt` | Yield at time of trade |
# | `trc_st` | TRACE status (T, X, C, W, Y) |
# | `msg_seq_nb` | Message sequence number |
# | `rpt_side_cd` | Reporting side (B=buy, S=sell) |
# | `cntra_mp_id` | Counterparty market participant ID |

# %%
trace_enh_dir = PULL_DIR / "trace_enhanced"
trace_enh = pl.scan_parquet(
    trace_enh_dir / "**/*.parquet",
    hive_partitioning=True,
    allow_missing_columns=True,
)
n_rows_enh = trace_enh.select(pl.len()).collect().item()
cols_enh = trace_enh.collect_schema().names()
print(f"Rows: {n_rows_enh:,}  |  Columns: {len(cols_enh)}")
print(f"Column names: {cols_enh}")

# %% [markdown]
# ### Example rows (first partition)

# %%
trace_enh.head(5).collect()

# %% [markdown]
# ### Date range

# %%
trace_enh.select(
    pl.col("trd_exctn_dt").min().alias("earliest"),
    pl.col("trd_exctn_dt").max().alias("latest"),
).collect()

# %% [markdown]
# ### Summary statistics — price & volume

# %%
trace_enh.select(
    pl.col("rptd_pr").mean().alias("mean_price"),
    pl.col("rptd_pr").std().alias("std_price"),
    pl.col("rptd_pr").median().alias("median_price"),
    pl.col("entrd_vol_qt").mean().alias("mean_volume"),
    pl.col("entrd_vol_qt").median().alias("median_volume"),
).collect()

# %% [markdown]
# ### Trades per month

# %%
enh_monthly = (
    trace_enh.with_columns(pl.col("trd_exctn_dt").alias("date"))
    .group_by(pl.col("date").dt.truncate("1mo"))
    .agg(pl.len().alias("n_trades"))
    .sort("date")
    .collect()
)

fig, ax = plt.subplots(figsize=(10, 3))
ax.bar(
    enh_monthly["date"].to_list(),
    enh_monthly["n_trades"].to_list(),
    width=25,
    color="steelblue",
    alpha=0.8,
)
ax.xaxis.set_major_locator(mdates.YearLocator(2))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
ax.set_ylabel("Trades")
ax.set_title("TRACE Enhanced — trades per month")
fig.tight_layout()
plt.show()

# %% [markdown]
# ---
# ## 14. TRACE Standard — `pulled/trace_standard/`
#
# Source script: `pull_trace_standard.py` | WRDS table: `trace.trace`
#
# Trade-level corporate bond transaction data from the Standard TRACE feed.
# Available from October 2024 onward. Same hive-partitioned storage as
# Enhanced. Uses a slightly different column set (e.g. `ascii_rptd_vol_tx`
# instead of `entrd_vol_qt`, `side` instead of `rpt_side_cd`).
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `cusip_id` | 9-character CUSIP bond identifier |
# | `trd_exctn_dt` | Trade execution date |
# | `trd_exctn_tm` | Trade execution time |
# | `rptd_pr` | Reported price (per $100 par) |
# | `ascii_rptd_vol_tx` | Reported volume (text field) |
# | `yld_pt` | Yield at time of trade |
# | `trc_st` | TRACE status |
# | `side` | Trade side |
# | `msg_seq_nb` | Message sequence number |

# %%
trace_std_dir = PULL_DIR / "trace_standard"
trace_std = pl.scan_parquet(
    trace_std_dir / "**/*.parquet",
    hive_partitioning=True,
    allow_missing_columns=True,
)
n_rows_std = trace_std.select(pl.len()).collect().item()
cols_std = trace_std.collect_schema().names()
print(f"Rows: {n_rows_std:,}  |  Columns: {len(cols_std)}")
print(f"Column names: {cols_std}")

# %% [markdown]
# ### Example rows

# %%
trace_std.head(5).collect()

# %% [markdown]
# ### Date range

# %%
trace_std.select(
    pl.col("trd_exctn_dt").min().alias("earliest"),
    pl.col("trd_exctn_dt").max().alias("latest"),
).collect()

# %%
