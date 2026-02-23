## Description

Enhanced TRACE daily bond pricing panel. This is the primary TRACE dataset covering 2002-present, processed through the Stage 0 Dick-Nielsen cleaning pipeline with decimal-shift correction and bounce-back filtering. Data is stored as hive-partitioned Parquet files under `_data/stage0/enhanced/year=YYYY/month=MM/data.parquet`.

Enhanced TRACE provides the most granular transaction data, including counterparty information. This dataset covers the majority of investment-grade and high-yield corporate bonds.

## Data Dictionary

- **cusip_id**: `category` 9-character CUSIP identifier
- **trd_exctn_dt**: `datetime64[ns]` Trade execution date
- **prc_ew**: `float32` Equal-weighted average price (% of par)
- **prc_vw**: `float32` Volume-weighted average price, dollar-weighted (% of par)
- **prc_vw_par**: `float32` Volume-weighted average price, par-weighted (% of par)
- **prc_first**: `float32` First trade price of the day (% of par)
- **prc_last**: `float32` Last trade price of the day (% of par)
- **prc_hi**: `float32` Highest price of the day (% of par)
- **prc_lo**: `float32` Lowest price of the day (% of par)
- **trade_count**: `int32` Number of trades
- **qvolume**: `float32` Par volume (millions USD)
- **dvolume**: `float32` Dollar volume (millions USD)
- **prc_bid**: `float32` Dealer bid price, value-weighted (% of par)
- **prc_ask**: `float32` Dealer ask price, value-weighted (% of par)
- **bid_count**: `int32` Number of dealer buys
- **ask_count**: `int32` Number of dealer sells
- **time_ew**: `float32` Average trade time (seconds after midnight)
- **time_last**: `int32` Last trade time (seconds after midnight)
- **bid_last**: `float32` Last dealer bid price (% of par)
- **bid_time_ew**: `float32` Average dealer bid time (seconds after midnight)
- **bid_time_last**: `int32` Last dealer bid time (seconds after midnight)
