use super::*;
use crate::imports::kraken::{read_ledgers, read_trades};
use crate::imports::wallet::{Txi, Txo, TxoInfo};
use crate::model::checkpoint::{Balances, CheckpointHeader, UtxoBalances};
use crate::model::events::{event_proceeds, EventAtom, EventSubType, GainPortion, GainTerm};
use crate::model::exchange_rate::{ExchangeRateMap, ExchangeRates};
use crate::model::{blockchain::Utxo, CapGainsWorksheet, Stats, Sums};
use crate::model::{kraken_amount::PoolUsdAmount, ledgers::rows::LedgerRowTypical};
use chrono::NaiveDateTime;
use gitver::GitverHashes;
use std::collections::BTreeSet;
use tracing_test::traced_test;

// use crate::model::{KrakenAmount, Stats, Sums, UsdAmount};

// Exchange rates are very important for properly reporting taxable events that involve various
// assets. There are two kinds of taxable events in the US: income and capital gains.
// (TODO: income reporting is implemented but not tested yet.)
//
// Taxable events for capital gains only occur on outgoing assets. A fee is a separate taxable
// event on the fee coin itself: the fee is valued at its definitional or market value when
// paid, against the cost basis of the splits that paid it. Some examples of when a capital gain
// occurs:
//
// - Selling an asset for USD.
// - Converting one asset into another asset.
// - Spending an asset for goods or services.
//
// The exchange rate for selling an asset for USD is what we call a definitional exchange rate.
// The sale sets the exchange rate for that transaction precisely. Converting an asset does not
// have a definitional exchange rate, since neither asset is USD. For these trades, we use an
// exchange rates database to lookup the exchange rate for the outgoing asset.
//
// ***The exchange rate for incoming assets is never needed for reporting taxable events.***
//
// Cost basis lookup and calculation happens when building `EventAtom`s by calling
// `BasisLifecycle::get_exchange_rate_at_acquisition()`.
//
// We do not include USD as an asset, since it is the monetary base against which all taxes are
// calculated.
//
// Having set context for this test, we can now verify these claims:
//
// Claim 1: Capital gains only occur on outgoing assets.
// Claim 2: A fee is a taxable event on the fee coin, evaluated at its value when paid.
// Claim 3: The exchange rate for incoming assets is not needed (until it goes out).
// Claim 4: Outgoing USD is not a capital gain.
fn setup() -> (State, ExchangeRates) {
    let state = State {
        header: CheckpointHeader {
            time: "now".to_string(),
            semver: "0.0.1".to_string(),
            gitver: GitverHashes::default(),
            latest_row_time: None,
        },
        on_chain_balances: UtxoBalances {
            btc: Utxo::from_iter([
                (
                    "abc1010def:1".to_string(),
                    FIFO::from_iter([PoolAsset::from_basis_row(&BasisRow {
                        synthetic_id: "c0dedbad:0".to_string(),
                        time: get_datetime("2012-09-08 11:32:12"),
                        asset: "XXBT".to_string(),
                        amount: Some(KrakenAmount::new("XXBT", "0.05000000").unwrap()),
                        exchange_rate: "103.13".parse().unwrap(),
                    })]),
                ),
                (
                    "012345fedc:0".to_string(),
                    FIFO::from_iter([PoolAsset::from_basis_row(&BasisRow {
                        synthetic_id: "feedbeef:0".to_string(),
                        time: get_datetime("2014-10-03 12:21:42"),
                        asset: "XXBT".to_string(),
                        amount: Some(KrakenAmount::new("XXBT", "0.18000000").unwrap()),
                        exchange_rate: "230.82".parse().unwrap(),
                    })]),
                ),
            ]),
            ..UtxoBalances::default()
        },
        exchange_balances: Balances {
            btc: FIFO::from_iter([PoolAsset::from_basis_row(&BasisRow {
                synthetic_id: "abc123:1".to_string(),
                time: get_datetime("2019-09-08 19:38:42"),
                asset: "XXBT".to_string(),
                amount: Some(KrakenAmount::new("XXBT", "0.20000000").unwrap()),
                exchange_rate: "10423.341672619628".parse().unwrap(),
            })]),
            ..Balances::default()
        },
        ..State::new(bdk::bitcoin::Network::Testnet)
    };

    let exchange_rates_db = ExchangeRates::from_raw(
        60 * 60 * 24 - 1,           // granularity
        ExchangeRateMap::default(), // BTC
        ExchangeRateMap::default(), // CHF
        ExchangeRateMap::default(), // ETH
        ExchangeRateMap::default(), // ETHW
        ExchangeRateMap::default(), // EUR
        ExchangeRateMap::default(), // JPY
        ExchangeRateMap::default(), // USDC
        ExchangeRateMap::default(), // USDT
    );

    (state, exchange_rates_db)
}

fn get_datetime(datetime: &str) -> DateTime<Utc> {
    NaiveDateTime::parse_from_str(datetime, "%F %T")
        .unwrap()
        .and_utc()
}

#[test]
#[traced_test]
fn test_handle_trade_exchange_rates_claim_1() {
    let _ = tracing_log::LogTracer::init();

    let (mut state, exchange_rates_db) = setup();
    let mut args = Args {
        gain_config: GainConfig {
            exchange_rates_db,
            bona_fide_residency: None, // TODO: Tests with residency
        },
        trades: HashMap::from([(
            "trade-1".to_string(),
            KrakenAmount::new("ZUSD", "34885.60").unwrap(),
        )]),
        pending_spends: Pending::default(),
        pending_withdrawals: Pending::default(),
        basis_lookup: BasisLookup::default(),
    };

    let lp = Rc::new(LedgerParsed::Trade {
        row_out: LedgerRowTypical {
            txid: "outgoing-1".to_string(),
            refid: "trade-1".to_string(),
            time: get_datetime("2023-11-06 19:34:12"),
            amount: KrakenAmount::new("XXBT", "-0.10000000").unwrap(),
            fee: KrakenAmount::new("XXBT", "0.00020000").unwrap(),
            balance: KrakenAmount::new("XXBT", "0.00000000").unwrap(),
        },
        row_in: LedgerRowTypical {
            txid: "incoming-1".to_string(),
            refid: "trade-1".to_string(),
            time: get_datetime("2023-11-06 19:34:12"),
            amount: KrakenAmount::new("ZUSD", "3488.56").unwrap(),
            fee: KrakenAmount::new("ZUSD", "0.00").unwrap(),
            balance: KrakenAmount::new("ZUSD", "0.00").unwrap(),
        },
    });

    let actual = state.handle_trade(&Rc::from("example-worksheet"), lp, &mut args);
    assert_eq!(actual.len(), 1);
    let event = &actual[0].as_ref().unwrap();
    assert_eq!(
        event.event_info.asset_out_exchange_rate,
        Some(UsdAmount::from("34885.60".parse::<FiatAmount>().unwrap())),
    );
    assert_eq!(
        event.event_info.asset_in_exchange_rate,
        Some(UsdAmount::from("1.0000".parse::<FiatAmount>().unwrap())),
    );
    assert_eq!(
        event_proceeds(&event.event_details),
        UsdAmount::from("3488.56".parse::<FiatAmount>().unwrap()),
    );

    // The event has two atoms: the trade atom (0.1 BTC) and the fee atom (0.0002 BTC).
    // The trade atom holds the full proceeds, and the fee atom is valued at the event's
    // definitional rate.
    assert_eq!(event.event_details.len(), 2);
    match &event.event_details[0] {
        EventAtom::Trade {
            asset_amount,
            proceeds,
            net_gain,
        } => {
            assert_eq!(asset_amount.to_decimal(), "-0.10000000".parse().unwrap());
            assert_eq!(
                *proceeds,
                UsdAmount::from("3488.56".parse::<FiatAmount>().unwrap()),
            );
            match net_gain {
                GainTerm::LongUs(us) => {
                    // 3488.56 - 1042.3341672619628
                    let expected =
                        UsdAmount::from("2446.2258327380372".parse::<FiatAmount>().unwrap());
                    assert_eq!(us.net_gain, expected);
                    let expected =
                        UsdAmount::from("1042.3341672619628".parse::<FiatAmount>().unwrap());
                    assert_eq!(us.basis, expected);
                    assert_eq!(us.basis_date, get_datetime("2019-09-08 19:38:42"));
                }
                _ => panic!("Unexpected gain term: {net_gain:#?}"),
            }
        }
        other => panic!("Expected trade atom, got {other:#?}"),
    }

    match &event.event_details[1] {
        EventAtom::Fee {
            asset_amount,
            proceeds,
            net_gain,
            ledger_row_id,
        } => {
            assert!(ledger_row_id.is_none());
            assert_eq!(asset_amount.to_decimal(), "-0.00020000".parse().unwrap());
            // 0.0002 * 34885.60
            assert_eq!(
                *proceeds,
                UsdAmount::from("6.97712".parse::<FiatAmount>().unwrap()),
            );
            match net_gain {
                GainTerm::LongUs(us) => {
                    // 0.0002 * 10423.341672619628
                    let expected =
                        UsdAmount::from("2.0846683345239256".parse::<FiatAmount>().unwrap());
                    assert_eq!(us.basis, expected);
                    // 6.97712 - 2.0846683345239256
                    let expected =
                        UsdAmount::from("4.8924516654760744".parse::<FiatAmount>().unwrap());
                    assert_eq!(us.net_gain, expected);
                    assert_eq!(us.basis_date, get_datetime("2019-09-08 19:38:42"));
                }
                _ => panic!("Unexpected gain term: {net_gain:#?}"),
            }
        }
        other => panic!("Expected fee atom, got {other:#?}"),
    }
}

/// Setup like [`setup`], but the exchange BTC pool has two splits. with different basis.
fn setup_two_splits() -> (State, ExchangeRates) {
    let state = State {
        header: CheckpointHeader {
            time: "now".to_string(),
            semver: "0.0.1".to_string(),
            gitver: GitverHashes::default(),
            latest_row_time: None,
        },
        on_chain_balances: UtxoBalances::default(),
        exchange_balances: Balances {
            btc: FIFO::from_iter([
                PoolAsset::from_basis_row(&BasisRow {
                    synthetic_id: "split-1:0".to_string(),
                    time: get_datetime("2019-09-08 19:38:42"),
                    asset: "XXBT".to_string(),
                    amount: Some(KrakenAmount::new("XXBT", "0.06000000").unwrap()),
                    exchange_rate: "100.00".parse().unwrap(),
                }),
                PoolAsset::from_basis_row(&BasisRow {
                    synthetic_id: "split-2:0".to_string(),
                    time: get_datetime("2024-03-01 12:00:00"),
                    asset: "XXBT".to_string(),
                    amount: Some(KrakenAmount::new("XXBT", "0.10000000").unwrap()),
                    exchange_rate: "200.00".parse().unwrap(),
                }),
            ]),
            ..Balances::default()
        },
        ..State::new(bdk::bitcoin::Network::Testnet)
    };

    let exchange_rates_db = ExchangeRates::from_raw(
        60 * 60 * 24 - 1,           // granularity
        ExchangeRateMap::default(), // BTC
        ExchangeRateMap::default(), // CHF
        ExchangeRateMap::default(), // ETH
        ExchangeRateMap::default(), // ETHW
        ExchangeRateMap::default(), // EUR
        ExchangeRateMap::default(), // JPY
        ExchangeRateMap::default(), // USDC
        ExchangeRateMap::default(), // USDT
    );

    (state, exchange_rates_db)
}

/// Selling 0.15 BTC spanning two splits (0.06 @ $100, 0.09 @ $200) for 15,000 ZUSD with a 0.003
/// BTC fee. Each trade atom holds the full proceeds of its split, and the fee atom holds the
/// fee value (300 USD). The sums table subtracts the fee value from the gain of the sale.
#[test]
#[traced_test]
fn test_multi_split_sale_fee() {
    let _ = tracing_log::LogTracer::init();

    let (mut state, exchange_rates_db) = setup_two_splits();
    let mut args = Args {
        gain_config: GainConfig {
            exchange_rates_db,
            bona_fide_residency: None,
        },
        trades: HashMap::from([(
            "trade-1".to_string(),
            // Definitional price: 15000 ZUSD for 0.15 BTC = 100000 ZUSD per BTC.
            KrakenAmount::new("ZUSD", "100000.00").unwrap(),
        )]),
        pending_spends: Pending::default(),
        pending_withdrawals: Pending::default(),
        basis_lookup: BasisLookup::default(),
    };

    let lp = Rc::new(LedgerParsed::Trade {
        row_out: LedgerRowTypical {
            txid: "outgoing-1".to_string(),
            refid: "trade-1".to_string(),
            time: get_datetime("2026-07-20 10:00:00"),
            amount: KrakenAmount::new("XXBT", "-0.15000000").unwrap(),
            fee: KrakenAmount::new("XXBT", "0.00300000").unwrap(),
            balance: KrakenAmount::new("XXBT", "0.00700000").unwrap(),
        },
        row_in: LedgerRowTypical {
            txid: "incoming-1".to_string(),
            refid: "trade-1".to_string(),
            time: get_datetime("2026-07-20 10:00:00"),
            amount: KrakenAmount::new("ZUSD", "15000.00").unwrap(),
            fee: KrakenAmount::new("ZUSD", "0.00").unwrap(),
            balance: KrakenAmount::new("ZUSD", "0.00").unwrap(),
        },
    });

    let actual = state.handle_trade(&Rc::from("example-worksheet"), lp, &mut args);
    assert_eq!(actual.len(), 1);
    let event = &actual[0].as_ref().unwrap();

    assert_eq!(
        event_proceeds(&event.event_details),
        UsdAmount::from("15000.00".parse::<FiatAmount>().unwrap()),
    );
    // Two trade atoms (one per split) and one fee atom.
    assert_eq!(event.event_details.len(), 3);

    match &event.event_details[0] {
        EventAtom::Trade {
            asset_amount,
            proceeds,
            net_gain,
        } => {
            assert_eq!(
                asset_amount,
                &KrakenAmount::new("XXBT", "-0.06000000").unwrap(),
            );
            // 0.06 * 100000
            assert_eq!(
                *proceeds,
                UsdAmount::from("6000.00".parse::<FiatAmount>().unwrap()),
            );
            match net_gain {
                GainTerm::LongUs(us) => {
                    assert_eq!(
                        us.basis,
                        UsdAmount::from("6.00".parse::<FiatAmount>().unwrap()),
                    );
                    assert_eq!(
                        us.net_gain,
                        UsdAmount::from("5994.00".parse::<FiatAmount>().unwrap()),
                    );
                    assert_eq!(us.basis_date, get_datetime("2019-09-08 19:38:42"));
                }
                other => panic!("Unexpected gain term: {other:#?}"),
            }
        }
        other => panic!("Expected trade atom, got {other:#?}"),
    }

    match &event.event_details[1] {
        EventAtom::Trade {
            asset_amount,
            proceeds,
            net_gain,
        } => {
            assert_eq!(
                asset_amount,
                &KrakenAmount::new("XXBT", "-0.09000000").unwrap(),
            );
            // 0.09 * 100000
            assert_eq!(
                *proceeds,
                UsdAmount::from("9000.00".parse::<FiatAmount>().unwrap()),
            );
            match net_gain {
                GainTerm::LongUs(us) => {
                    assert_eq!(
                        us.basis,
                        UsdAmount::from("18.00".parse::<FiatAmount>().unwrap()),
                    );
                    assert_eq!(
                        us.net_gain,
                        UsdAmount::from("8982.00".parse::<FiatAmount>().unwrap()),
                    );
                    assert_eq!(us.basis_date, get_datetime("2024-03-01 12:00:00"));
                }
                other => panic!("Unexpected gain term: {other:#?}"),
            }
        }
        other => panic!("Expected trade atom, got {other:#?}"),
    }

    match &event.event_details[2] {
        EventAtom::Fee {
            asset_amount,
            proceeds,
            net_gain,
            ledger_row_id,
        } => {
            assert!(ledger_row_id.is_none());
            assert_eq!(
                asset_amount,
                &KrakenAmount::new("XXBT", "-0.00300000").unwrap(),
            );
            // 0.003 * 100000 (definitional rate)
            assert_eq!(
                *proceeds,
                UsdAmount::from("300.00".parse::<FiatAmount>().unwrap()),
            );
            match net_gain {
                GainTerm::LongUs(us) => {
                    // The fee is consumed from split 2 after the trade: 0.003 * 200
                    assert_eq!(
                        us.basis,
                        UsdAmount::from("0.60".parse::<FiatAmount>().unwrap()),
                    );
                    assert_eq!(
                        us.net_gain,
                        UsdAmount::from("299.40".parse::<FiatAmount>().unwrap()),
                    );
                    assert_eq!(us.basis_date, get_datetime("2024-03-01 12:00:00"));
                }
                other => panic!("Unexpected gain term: {other:#?}"),
            }
        }
        other => panic!("Expected fee atom, got {other:#?}"),
    }

    let owned_event = (**event).clone();
    let sums = CapGainsWorksheet::new(vec![owned_event]).sums();
    sums.assert_error_check();
    // The trade atoms (15000) and the fee atom (300).
    assert_eq!(
        sums.us_long_trade_proceeds(),
        UsdAmount::from("15300.00".parse::<FiatAmount>().unwrap()),
    );
    assert_eq!(
        sums.us_long_trade_basis(),
        UsdAmount::from("24.60".parse::<FiatAmount>().unwrap()),
    );
    // 5994 + 8982 + 299.40
    assert_eq!(
        sums.us_long_trade_gain(),
        UsdAmount::from("15275.40".parse::<FiatAmount>().unwrap()),
    );
    assert_eq!(
        sums.us_long_fees(),
        UsdAmount::from("300.00".parse::<FiatAmount>().unwrap()),
    );
    // 15300 - 24.60 - 300
    assert_eq!(
        sums.gains_us_long(),
        UsdAmount::from("14975.40".parse::<FiatAmount>().unwrap()),
    );
    assert_eq!(sums.gains_us_short(), UsdAmount::default(),);

    // 0.10 - 0.09 - 0.003 = 0.007 of split 2 remains.
    let splits: Vec<_> = state.exchange_balances.btc.iter().collect();
    assert_eq!(splits.len(), 1);
    assert_eq!(
        KrakenAmount::from(splits[0].amount),
        KrakenAmount::new("XXBT", "0.00700000").unwrap(),
    );
}

#[test]
#[traced_test]
fn test_match_one_tx_transfer_or_move() {
    let _ = tracing_log::LogTracer::init();

    let (mut state, _exchange_rates_db) = setup();
    let exchange_rates_db = ExchangeRates::from_raw(
        60 * 60 * 24 - 1, // granularity
        ExchangeRateMap::from_iter([(
            get_datetime("2023-11-06 00:00:00").timestamp() as u64,
            UsdAmount::from("50000".parse::<FiatAmount>().unwrap()),
        )]),
        ExchangeRateMap::default(), // CHF
        ExchangeRateMap::default(), // ETH
        ExchangeRateMap::default(), // ETHW
        ExchangeRateMap::default(), // EUR
        ExchangeRateMap::default(), // JPY
        ExchangeRateMap::default(), // USDC
        ExchangeRateMap::default(), // USDT
    );
    let mut args = Args {
        gain_config: GainConfig {
            exchange_rates_db,
            bona_fide_residency: None, // TODO: Tests with residency
        },
        trades: HashMap::from([(
            "trade-1".to_string(),
            KrakenAmount::new("ZUSD", "34885.60").unwrap(),
        )]),
        pending_spends: Pending::default(),
        pending_withdrawals: Pending::default(),
        basis_lookup: BasisLookup::default(),
    };

    let tx = Tx {
        time: get_datetime("2023-11-06 19:34:12"),
        asset: AssetName::Btc,
        txid: "MY-TXID".to_string(),
        ins: vec![
            Txi {
                external_id: "abc1010def:1".to_string(),
                amount: None,
                mine: true,
            },
            Txi {
                external_id: "012345fedc:0".to_string(),
                amount: None,
                mine: true,
            },
        ],
        outs: vec![Txo {
            amount: KrakenAmount::new("XXBT", "0.20000000").unwrap(),
            mine: true,
            wallet_info: BTreeSet::from_iter([TxoInfo {
                account: "PJFargo".to_string(),
                note: "".to_string(),
            }]),
        }],
        tx_type: None,
        exchange_rate: None,
    };
    let mut actual = state.match_one_tx(&Rc::from("example-worksheet"), tx, &mut args);
    assert_eq!(actual.len(), 1);
    let actual = actual.remove(0).unwrap();
    // The move has one atom: the fee atom (0.03 BTC), valued at the market rate.
    assert_eq!(actual.event_details.len(), 1);
    match &actual.event_details[0] {
        EventAtom::Fee {
            asset_amount,
            proceeds,
            net_gain,
            ledger_row_id,
        } => {
            assert!(ledger_row_id.is_none());
            assert_eq!(
                asset_amount,
                &KrakenAmount::new("XXBT", "-0.03000000").unwrap(),
            );
            // 0.03 * 50000
            assert_eq!(
                *proceeds,
                UsdAmount::from("1500".parse::<FiatAmount>().unwrap()),
            );
            match net_gain {
                GainTerm::LongUs(us) => {
                    assert_eq!(
                        us.basis,
                        UsdAmount::from("6.9246".parse::<FiatAmount>().unwrap()),
                    );
                    assert_eq!(us.basis_date, get_datetime("2014-10-03 12:21:42"));
                    assert_eq!(us.basis_synthetic_id, "feedbeef:0");
                    // 1500 - 6.9246
                    assert_eq!(
                        us.net_gain,
                        UsdAmount::from("1493.0754".parse::<FiatAmount>().unwrap()),
                    );
                }
                _ => panic!("Unexpected gain term: {net_gain:#?}"),
            }
        }
        other => panic!("Expected fee atom, got {other:#?}"),
    }

    // Check the state of the on-chain BTCs after processing the transaction.
    let utxos = state
        .on_chain_balances
        .btc
        .into_iter()
        .map(|(key, value)| {
            (
                key,
                value
                    .into_iter()
                    .map(|pool_asset| format!("{pool_asset:?}"))
                    .collect(),
            )
        })
        .collect::<HashMap<_, Vec<_>>>();

    assert_eq!(
        utxos,
        HashMap::from_iter([(
            "MY-TXID:0".to_string(),
            Vec::from_iter([
                // TODO: Comparing a string representation isn't ideal, but good enough for now.
                concat!(
                    "PoolAsset { amount: BitcoinAmount(0.05000000), lifecycle: BasisLifecycle { ",
                    "synthetic_id: \"c0dedbad:0\", resolved_origin: Bucket(Bucket { ",
                    "synthetic_id: \"c0dedbad:0\", time: 2012-09-08T11:32:12Z, ",
                    "amount: Btc(BitcoinAmount(0.05000000)), exchange_rate: ",
                    "UsdAmount(FiatAmount(103.13)) }) } }",
                )
                .to_string(),
                concat!(
                    "PoolAsset { amount: BitcoinAmount(0.15000000), lifecycle: BasisLifecycle { ",
                    "synthetic_id: \"feedbeef:0\", resolved_origin: Bucket(Bucket { ",
                    "synthetic_id: \"feedbeef:0\", time: 2014-10-03T12:21:42Z, ",
                    "amount: Btc(BitcoinAmount(0.18000000)), exchange_rate: ",
                    "UsdAmount(FiatAmount(230.82)) }) } }",
                )
                .to_string(),
            ]),
        )])
    );
}

// Fixture-driven tests from `fixtures/fee-cases/`.
//
// Each fixture is a Kraken-format scenario (ledgers + trades) exercising fee handling:
// - `lt-sale-st-fee`: FIFO sells the LT lot; the fee coin comes from the ST lot.
// - `st-sale-lt-fee`: FIFO sells the ST lot; the fee coin comes from the LT lot.
// - `margin-rollover`: `st-sale-lt-fee` plus a margin position whose open and rollover fees are
//   paid in BTC from the remaining LT lot.

/// Construct a UsdAmount from a string.
///
/// The string should have a precision of 4 decimal places, like "0.0000".
fn usd(s: &str) -> UsdAmount {
    UsdAmount::from(s.parse::<FiatAmount>().unwrap())
}

/// Seed BTC ($1,000) and ETH ($50) daily rates at 00:00 UTC on each given day.
fn rates(btc_days: &[&str], eth_days: &[&str]) -> ExchangeRates {
    let ts = |day: &str| get_datetime(&format!("{day} 00:00:00")).timestamp() as u64;
    ExchangeRates::from_raw(
        60 * 60 * 24 - 1, // granularity
        btc_days.iter().map(|d| (ts(d), usd("1000.0000"))).collect(),
        ExchangeRateMap::default(), // CHF
        eth_days.iter().map(|d| (ts(d), usd("50.0000"))).collect(),
        ExchangeRateMap::default(), // ETHW
        ExchangeRateMap::default(), // EUR
        ExchangeRateMap::default(), // JPY
        ExchangeRateMap::default(), // USDC
        ExchangeRateMap::default(), // USDT
    )
}

/// The off-exchange BTC deposit basis: 1.00 BTC bought on 2020-05-15 at $10/BTC (LT).
/// Keyed by the deposit row's txid, as `handle_deposit_inner` looks it up.
fn basis_row(deposit_txid: &str) -> BasisRow {
    BasisRow {
        synthetic_id: deposit_txid.to_string(),
        time: get_datetime("2020-05-15 12:00:00"),
        asset: "XXBT".to_string(),
        amount: Some(KrakenAmount::new("XXBT", "1.00000000").unwrap()),
        exchange_rate: "10".parse().unwrap(),
    }
}

/// Run `State::resolve` on a fixture, returning the state and events in chronological order.
fn run(name: &str, rates: ExchangeRates, basis: Vec<BasisRow>) -> (State, Vec<Event>) {
    let mut stats = Stats::default();
    let dir = format!("fixtures/fee-cases/{name}");
    let trades = read_trades(&mut stats, format!("{dir}/kraken-trades.csv")).unwrap();
    let ledgers = read_ledgers(&mut stats, format!("{dir}/kraken-ledgers.csv"))
        .unwrap()
        .parse(&trades)
        .unwrap();

    let mut state = State::new(bdk::bitcoin::Network::Testnet);
    let gain_config = GainConfig {
        exchange_rates_db: rates,
        bona_fide_residency: None,
    };
    let ledgers = HashMap::from([(Rc::from(name), ledgers)]);
    let resolved = state.resolve(
        HashMap::new(),
        ledgers,
        gain_config,
        trades,
        FIFO::from_iter(basis),
    );
    let events = resolved
        .inner
        .into_iter()
        .collect::<Result<Vec<Event>, _>>()
        .unwrap();
    (state, events)
}

/// The sum of event_proceeds() across the events.
fn total_proceeds(events: &[Event]) -> UsdAmount {
    events.iter().fold(UsdAmount::default(), |acc, event| {
        acc + event_proceeds(&event.event_details)
    })
}

/// Build the worksheet sums for the events, and check the identity in each cell.
fn sums(events: Vec<Event>) -> Sums {
    let sums = CapGainsWorksheet::new(events).sums();
    sums.assert_error_check();
    sums
}

fn find_event<'a>(
    events: &'a [Event],
    date: &str,
    subtype: impl Fn(&EventSubType) -> bool,
) -> &'a Event {
    let date = get_datetime(date);
    events
        .iter()
        .find(|e| e.event_info.event_date == date && subtype(&e.event_info.event_subtype))
        .unwrap_or_else(|| panic!("no event on {date}"))
}

fn atoms_of(event: &Event, matches: impl Fn(&EventAtom) -> bool) -> Vec<&EventAtom> {
    event.event_details.iter().filter(|a| matches(a)).collect()
}

fn long_us(net_gain: &GainTerm) -> &GainPortion {
    match net_gain {
        GainTerm::LongUs(us) => us,
        other => panic!("expected LongUs, got {other:?}"),
    }
}

fn short_us(net_gain: &GainTerm) -> &GainPortion {
    match net_gain {
        GainTerm::ShortUs(us) => us,
        other => panic!("expected ShortUs, got {other:?}"),
    }
}

/// Assert the XETHXXBT sale event: one trade atom and one fee atom.
///
/// Each tuple is the expected (basis_date, basis, gain, is_long_term) for an atom.
fn assert_sale<'a>(
    events: &'a [Event],
    trade: (&str, &str, &str, bool),
    fee: (&str, &str, &str, bool),
) -> &'a Event {
    let sale = find_event(events, "2026-07-20 14:00:00", |s| {
        matches!(s, EventSubType::Trade)
    });

    let trade_atoms = atoms_of(sale, |a| matches!(a, EventAtom::Trade { .. }));
    assert_eq!(trade_atoms.len(), 1);
    match trade_atoms[0] {
        EventAtom::Trade {
            asset_amount,
            proceeds,
            net_gain,
        } => {
            assert_eq!(asset_amount.to_decimal(), "-1.00000000".parse().unwrap());
            // 1.00 BTC * $1,000. The fee value (0.008 BTC * $1,000) is in the fee atom.
            assert_eq!(*proceeds, usd("1000.0000"));
            let us = if trade.3 {
                long_us(net_gain)
            } else {
                short_us(net_gain)
            };
            assert_eq!(us.basis_date, get_datetime(trade.0));
            assert_eq!(us.basis, usd(trade.1));
            assert_eq!(us.net_gain, usd(trade.2));
        }
        other => panic!("expected trade atom, got {other:?}"),
    }

    let fee_atoms = atoms_of(sale, |a| matches!(a, EventAtom::Fee { .. }));
    assert_eq!(fee_atoms.len(), 1);
    match fee_atoms[0] {
        EventAtom::Fee {
            asset_amount,
            proceeds,
            net_gain,
            ledger_row_id,
        } => {
            assert!(ledger_row_id.is_none());
            assert_eq!(asset_amount.to_decimal(), "-0.00800000".parse().unwrap());
            assert_eq!(*proceeds, usd("8.0000"));
            let us = if fee.3 {
                long_us(net_gain)
            } else {
                short_us(net_gain)
            };
            assert_eq!(us.basis_date, get_datetime(fee.0));
            assert_eq!(us.basis, usd(fee.1));
            assert_eq!(us.net_gain, usd(fee.2));
        }
        other => panic!("expected fee atom, got {other:?}"),
    }

    sale
}

/// Assert a single-split exchange balance: amount and acquisition rate.
fn assert_pool(
    pool: &FIFO<PoolAsset<BitcoinAmount>>,
    db: &ExchangeRates,
    amount: &str,
    rate: &str,
) {
    let splits: Vec<_> = pool.iter().collect();
    assert_eq!(splits.len(), 1);
    let split = &splits[0];
    assert_eq!(
        KrakenAmount::from(split.amount),
        KrakenAmount::new("XXBT", amount).unwrap(),
    );
    let actual_rate = split
        .lifecycle
        .get_exchange_rate_at_acquisition(db)
        .unwrap();
    assert_eq!(actual_rate, usd(rate));
}

#[test]
fn lt_sale_st_fee() {
    let (state, events) = run(
        "lt-sale-st-fee",
        rates(&["2026-07-20"], &["2026-07-20"]),
        vec![],
    );

    // The 2020 split's fiat fee is capitalized: basis $10.08/BTC (LT).
    // The 2026 split's fiat fee is capitalized: basis $1,008/BTC (ST).
    // The sale disposes 1.008 BTC:
    // - 1.000 from the 2020 split (trade atom)
    // - 0.008 from the 2026 split (fee atom)
    assert_sale(
        &events,
        ("2020-05-15 11:00:00", "10.0800", "989.9200", true),
        ("2026-01-10 10:00:00", "8.0640", "-0.0640", false),
    );

    let db = rates(&["2026-07-20"], &["2026-07-20"]);
    let sums = sums(events);
    // The $8 fee is an expense of the LT sale: 1000 - 10.08 - 8.
    assert_eq!(sums.us_long_fees(), usd("8.0000"));
    assert_eq!(sums.gains_us_long(), usd("981.9200"));
    assert_eq!(sums.gains_us_short(), usd("-0.0640"));

    // Remaining BTC split: 0.992 at $1,008. ETH basis: 20 ETH at $50.
    assert_pool(&state.exchange_balances.btc, &db, "0.99200000", "1008.0000");
    // The capitalized USD buy fees must still leave the USD pool, or it drifts from the
    // ledger balance and the next year's `assert_balance` fails.
    assert_eq!(
        state.exchange_balances.usd.amount(),
        PoolUsdAmount::default(),
    );
    let eth: Vec<_> = state.exchange_balances.eth.iter().collect();
    assert_eq!(eth.len(), 1);
    assert_eq!(
        KrakenAmount::from(eth[0].amount),
        KrakenAmount::new("XETH", "20.0000000000").unwrap(),
    );
    let rate = eth[0]
        .lifecycle
        .get_exchange_rate_at_acquisition(&db)
        .unwrap();
    assert_eq!(rate, usd("50.0000"));
}

#[test]
fn st_sale_lt_fee() {
    let (state, events) = run(
        "st-sale-lt-fee",
        rates(&["2026-07-20"], &["2026-07-20"]),
        vec![basis_row("ca5e02-1ed9e-000004")],
    );

    // FIFO: the 2026-01-10 exchange split ($1,008, ST) entered first and is sold;
    // the 2020-05-15 off-exchange split ($10, LT) pays the fee.
    assert_sale(
        &events,
        ("2026-01-10 10:00:00", "1008.0000", "-8.0000", false),
        ("2020-05-15 12:00:00", "0.0800", "7.9200", true),
    );

    let db = rates(&["2026-07-20"], &["2026-07-20"]);
    let sums = sums(events);
    // The $8 fee is an expense of the ST sale: 1000 - 1008 - 8.
    assert_eq!(sums.us_short_fees(), usd("8.0000"));
    assert_eq!(sums.gains_us_long(), usd("7.9200"));
    assert_eq!(sums.gains_us_short(), usd("-16.0000"));

    // The remaining BTC is the tail of the LT split (the ST split was fully sold).
    assert_pool(&state.exchange_balances.btc, &db, "0.99200000", "10.0000");
    // The capitalized USD buy fee must still leave the USD pool, or it drifts from the
    // ledger balance and the next year's `assert_balance` fails.
    assert_eq!(
        state.exchange_balances.usd.amount(),
        PoolUsdAmount::default(),
    );
    let eth: Vec<_> = state.exchange_balances.eth.iter().collect();
    assert_eq!(eth.len(), 1);
    let rate = eth[0]
        .lifecycle
        .get_exchange_rate_at_acquisition(&db)
        .unwrap();
    assert_eq!(rate, usd("50.0000"));
}

#[test]
fn margin_rollover() {
    let (state, events) = run(
        "margin-rollover",
        rates(&["2026-07-20", "2026-07-25", "2026-07-26"], &["2026-07-20"]),
        vec![basis_row("ca5e03-1ed9e-000004")],
    );

    // Sale: same as st-sale-lt-fee.
    assert_sale(
        &events,
        ("2026-01-10 10:00:00", "1008.0000", "-8.0000", false),
        ("2020-05-15 12:00:00", "0.0800", "7.9200", true),
    );

    // Margin open fee: 0.02 BTC from the LT split. A carrying cost with no deduction: a
    // `Fee` atom (not `InvestmentFee`) in an event that is not a sale, so the fee value is
    // not an expense of a sale.
    let margin_open = find_event(&events, "2026-07-25 08:00:00", |s| {
        matches!(s, EventSubType::MarginOpen)
    });
    let fee_atoms = atoms_of(margin_open, |a| matches!(a, EventAtom::Fee { .. }));
    assert_eq!(fee_atoms.len(), 1);
    match fee_atoms[0] {
        EventAtom::Fee {
            asset_amount,
            proceeds,
            net_gain,
            ledger_row_id,
        } => {
            assert!(ledger_row_id.is_none());
            assert_eq!(asset_amount.to_decimal(), "-0.02000000".parse().unwrap());
            assert_eq!(*proceeds, usd("20.0000"));
            let us = long_us(net_gain);
            assert_eq!(us.basis, usd("0.2000"));
            assert_eq!(us.net_gain, usd("19.8000"));
        }
        other => panic!("expected fee atom, got {other:?}"),
    }

    // Five rollover fees: 0.02 BTC each from the LT split. Investment interest expense.
    let rollover_dates = [
        "2026-07-25 12:00:00",
        "2026-07-25 16:00:00",
        "2026-07-25 20:00:00",
        "2026-07-26 00:00:00",
        "2026-07-26 04:00:00",
    ];
    for date in rollover_dates {
        let rollover = find_event(&events, date, |s| matches!(s, EventSubType::MarginRollover));
        let fee_atoms = atoms_of(rollover, |a| matches!(a, EventAtom::InvestmentFee { .. }));
        assert_eq!(fee_atoms.len(), 1);
        match fee_atoms[0] {
            EventAtom::InvestmentFee {
                asset_amount,
                proceeds,
                net_gain,
                ledger_row_id,
            } => {
                assert!(ledger_row_id.is_none());
                assert_eq!(asset_amount.to_decimal(), "-0.02000000".parse().unwrap());
                assert_eq!(*proceeds, usd("20.0000"));
                let us = long_us(net_gain);
                assert_eq!(us.basis, usd("0.2000"));
                assert_eq!(us.net_gain, usd("19.8000"));
            }
            other => panic!("expected investment fee atom, got {other:?}"),
        }
    }

    // Total proceeds: 1.00 BTC at the defined rate of $1,000.
    assert_eq!(total_proceeds(&events), usd("1000.0000"));

    let db = rates(&["2026-07-20", "2026-07-25", "2026-07-26"], &["2026-07-20"]);
    let sums = sums(events);
    // LT fee coin gains: 7.92 (sale fee) + 19.80 (open fee) + 5 * 19.80 (rollover fees).
    assert_eq!(sums.us_long_trade_gain(), usd("126.7200"));
    // The open fee and the rollover fees are not expenses of a sale. The rollover fee values
    // ($100) are investment interest, a Form 4952 item outside the capital gain.
    assert_eq!(sums.us_long_fees(), usd("0.0000"));
    assert_eq!(sums.gains_us_long(), usd("126.7200"));
    // ST is the sale's -$8 loss minus the $8 sale fee.
    assert_eq!(sums.gains_us_short(), usd("-16.0000"));

    // Remaining BTC split: 0.872 at $10.
    assert_pool(&state.exchange_balances.btc, &db, "0.87200000", "10.0000");
    // The capitalized USD buy fee must still leave the USD pool, or it drifts from the
    // ledger balance and the next year's `assert_balance` fails.
    assert_eq!(
        state.exchange_balances.usd.amount(),
        PoolUsdAmount::default(),
    );

    // ETH basis: 20 ETH at the defined rate of $50, i.e. $1,000.
    let eth: Vec<_> = state.exchange_balances.eth.iter().collect();
    assert_eq!(eth.len(), 1);
    let rate = eth[0]
        .lifecycle
        .get_exchange_rate_at_acquisition(&db)
        .unwrap();
    assert_eq!(rate, usd("50.0000"));
}

#[test]
fn margin_open_two_row_fee() {
    let (_state, events) = run(
        "margin-open-two-row-fee",
        rates(&["2026-07-25"], &[]),
        vec![basis_row("ca5e04-1ed9e-000002")],
    );

    let open = find_event(&events, "2026-07-25 08:00:00", |s| {
        matches!(s, EventSubType::MarginOpen)
    });

    // The event's row is the first ledger row (the one that pays the USD fee).
    assert_eq!(open.event_info.ledger_row_id, "ca5e04-1ed9e-000003");
    assert_eq!(open.event_details.len(), 2);

    let fee_atoms = atoms_of(open, |a| matches!(a, EventAtom::Fee { .. }));
    assert_eq!(fee_atoms.len(), 2);

    // First row: 0.50 USD fee. Definitional $1 both ways: zero gain, ST. The fee is paid
    // on the event's own ledger row, so it carries no row override.
    match fee_atoms[0] {
        EventAtom::Fee {
            asset_amount,
            proceeds,
            net_gain,
            ledger_row_id,
        } => {
            assert!(ledger_row_id.is_none());
            assert_eq!(asset_amount, &KrakenAmount::new("ZUSD", "-0.5000").unwrap(),);
            assert_eq!(*proceeds, usd("0.5000"));
            let us = short_us(net_gain);
            assert_eq!(us.basis, usd("0.5000"));
            assert_eq!(us.net_gain, usd("0.0000"));
        }
        other => panic!("expected fee atom, got {other:?}"),
    }

    // Second row: 0.0001 BTC fee ($0.10) against the $10/BTC LT deposit: $0.0990 gain.
    // The atom must carry its own ledger row ID, not the event's.
    match fee_atoms[1] {
        EventAtom::Fee {
            asset_amount,
            proceeds,
            net_gain,
            ledger_row_id,
        } => {
            assert_eq!(ledger_row_id.as_deref(), Some("ca5e04-1ed9e-000004"));
            assert_eq!(
                asset_amount,
                &KrakenAmount::new("XXBT", "-0.00010000").unwrap(),
            );
            assert_eq!(*proceeds, usd("0.1000"));
            let us = long_us(net_gain);
            assert_eq!(us.basis, usd("0.0010"));
            assert_eq!(us.net_gain, usd("0.0990"));
        }
        other => panic!("expected fee atom, got {other:?}"),
    }

    assert_eq!(total_proceeds(&events), usd("0.0000"));

    let sums = sums(events);
    assert_eq!(sums.gains_us_long(), usd("0.0990"));
    assert_eq!(sums.gains_us_short(), usd("0.0000"));
}

#[test]
#[traced_test]
fn concatenate_distinct_gives_byte_order() {
    // With a HashSet, this test passes on 1 of 720 runs.
    let names = ["fox", "echo", "delta", "charlie", "bravo", "alpha", "delta"];
    assert_eq!(
        concatenate_distinct(names.into_iter()),
        "alpha, bravo, charlie, delta, echo, fox"
    );
}
