# `ohlc-ness`

![Loch Ness Urquhart Castle](https://upload.wikimedia.org/wikipedia/commons/5/51/LochNessUrquhart.jpg)

`ohlc-ness` (pronounced like "Loch Ness") is an application that automates most of the exchange rate database building by sourcing data from various public APIs. `taxcount` uses the database only when an exchange rate is required.


## Update Exchange Rates DB

The `taxcount` exchange rates DB can be updated each year with the `update-year.sh` shell script. It requires `bash`, and can be run with an optional year to collect exchange rates for the supported trade pairs.

```bash
./update-year.sh 2024
```


## Kraken OHLC siphon mode

```
$ mkdir -p kraken-2022
$ cargo run -p ohlc-ness -- --mode get_ohlc --dir ./kraken-2022/
```

Uses the [Kraken public `OHLC` API](https://docs.kraken.com/rest/#tag/Market-Data/operation/getOHLCData) to siphon daily OHLC data starting at a given timestamp (`--since` arg).

The raw `get_ohlc` data contains up to 720 samples, the last sample is always the most recent available. It is not possible to go back in time more than 720 samples. This is enough for _almost_ 2 years of daily OHLC data.

For older data, the `get_trades` API must be used.


## Bitstamp OHLC siphon mode

```
$ mkdir -p bitstamp-2022
$ cargo run -p ohlc-ness -- --mode get_ohlc --api bitstamp --since 1640995200 --end 1672531200 --dir ./kraken-2022/
```

Uses the [Bitstamp public `OHLC` API](https://www.bitstamp.net/api/#tag/Market-info/operation/GetOHLCData) to siphon daily or hourly OHLC data starting at a given timestamp (`--since` arg).

The raw `get_ohlc` data contains up to 1000 samples per JSON file. JSON files will be created repeatedly until the timestamp reaches the `--end` arg.


## Trades siphon mode

```
$ mkdir -p kraken-2022
$ cargo run -p ohlc-ness -- --mode get_trades --since 1640908800 --end 1672531200 --dir ./kraken-2022/
```

The [Kraken public `Trades` API](https://docs.kraken.com/rest/#tag/Market-Data/operation/getRecentTrades) will be called in sequence until the `--end` time is reached in the last data sample.

**WARNING:** This mode operates very slowly to not trigger the Kraken API rate limits. It will download _one file per second_. The required disk space depends on the number of trades within the given time range. For instance, all of 2021 requires 2.3 GB and over 17,000 JSON files.

To ensure all trades are captured for the starting and ending days, it is wise to use a timestamp for `--since` that is one day behind, and an `--end` that is one day ahead. The example above uses `2021-12-31` and `2023-01-01` for the time range that covers all of 2022.


### Restarting failed siphons

The public API may return errors on occasion. Any error will cause the script to exit. It can be restarted, picking up where it left off, by setting `--since` appropriately. Files in the work directory will be named `trades_{since}.json`. Some bash tricks can be used to get the most recent timestamp from the work directory:

```bash
$ ls kraken-2022/trades_*.json | sort -r | head -n 1 | egrep -o '_[0-9]+\.json' | egrep -o '[0-9]+'
```

1. Find all files matching the naming convention in the work directory.
2. Sort them.
3. Get the first one.
4. Isolate the filename from the full path.
5. Isolate the timestamp.

The script can accept timestamps in seconds or nanoseconds units. The long timestamps in the filenames use nanosecond units. See `--help` for more info.


## Fiat siphon mode

```
$ mkdir -p fred-eurusd-2022
$ cargo run -p ohlc-ness -- --mode get_fiat --pair EURUSD --since 1640908800 --end 1672617600 --dir ./fred-eurusd-2022/
```

Uses the [FRED public API](https://fred.stlouisfed.org/categories/94) to siphon daily average fiat price quotes. The source data is inherently missing weekends and sometimes contains gaps with explicit `.` columns. All gaps are substituted by the last known populated row.

As with the `get_trades` mode, you should pad the `--since` and `--end` timestamps to ensure you are pulling all of the data. The timestamps may need to be padded more than one day to account for missing data on weekends. The data should be cleaned up to remove the padding before it is included as a database file.


## Converting the data to a `taxcount` DB

```
$ cargo run -p ohlc-ness -- --mode taxcount --since 1640995200 --end 1672531200 --dir ./kraken-2022/
```

The `taxcount` mode can be used to convert the output JSON files from either `ohlc` or `trades` modes into a RON database for `taxcount`. The data will be saved to a file named `db.ron` in the `--dir` work directory.

The RON database maps days (Unix timestamp) to daily [VWAP](https://en.wikipedia.org/wiki/Volume-weighted_average_price).

**WARNING:** The `taxcount` mode does not attempt to de-duplicate the input data. If the work directory contains both `ohlc` and `trades` JSON files, the `ohlc` data will take priority. If any `trades` files have duplicate `trade_id`s, they will not be de-duped automatically and the resulting VWAPs will be invalid.

Use `--since` and `--end` to filter out unnecessary trades. `--since` is inclusive and `--end` is exclusive. To capture an entire year, you can use January 1st for both timestamps, with `--end` having one year more than `--since`. E.g. `2022-01-01` to `2023-01-01` covering all of 2022.


## Converting the data to `ohlcv`

```
$ cargo run -p ohlc-ness -- --mode ohlcv --buckets hourly --dir ./kraken-2022/
```

The data can be aggregated into daily and hourly OHLCV buckets.
