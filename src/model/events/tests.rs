// 10 / incorrect blank fee basis for all fee atoms of some trades
// Implementation Plan :: Step 2

use super::*;
use crate::basis::{BasisLifecycle, Bucket};
use crate::model::exchange_rate::ExchangeRateMap;
use crate::model::kraken_amount::{BitcoinAmount, PoolUsdAmount};
use chrono::TimeZone;
use tracing_test::traced_test;

/// The length of one day in seconds, minus 1 second.  ExchangeRates::from_raw() gets this value as
/// its first argument, for a database with one exchange rate for each day.
const ONE_DAY: u64 = 60 * 60 * 24 - 1;

/// The BTC exchange rate at the move date, in USD.
const MOVE_DATE_RATE: i64 = 300;

/// The BTC exchange rate at the event date, in USD.
const EVENT_RATE: i64 = 1000;

fn usd(n: i64) -> UsdAmount {
    UsdAmount::from_int(n)
}

fn date(year: i32, month: u32, day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).unwrap()
}

fn move_date() -> DateTime<Utc> {
    date(2024, 2, 24)
}

/// A GainConfig with the move date, and with one BTC exchange rate at the move date.
fn gain_config() -> GainConfig {
    let btc = ExchangeRateMap::from([(move_date().timestamp() as u64, usd(MOVE_DATE_RATE))]);

    GainConfig {
        exchange_rates_db: ExchangeRates::from_raw(
            ONE_DAY,
            btc,
            ExchangeRateMap::new(),
            ExchangeRateMap::new(),
            ExchangeRateMap::new(),
            ExchangeRateMap::new(),
            ExchangeRateMap::new(),
            ExchangeRateMap::new(),
            ExchangeRateMap::new(),
        ),
        bona_fide_residency: Some(move_date()),
    }
}

/// A BTC split with an acquisition rate of `rate` USD for each BTC.
fn btc_split(
    amount: &str,
    basis_date: DateTime<Utc>,
    rate: i64,
) -> PoolAssetNonSplittable<BitcoinAmount> {
    let amount = KrakenAmount::new("XXBT", amount).unwrap();
    let bucket = Bucket {
        synthetic_id: "basis-id".to_string(),
        time: basis_date,
        amount,
        exchange_rate: usd(rate),
    };

    PoolAssetNonSplittable {
        amount: BitcoinAmount::try_from(amount).unwrap(),
        lifecycle: BasisLifecycle::lifecycle_from_bucket(bucket),
    }
}

/// A USD split with the base currency as its origin.
fn usd_split(amount: &str) -> PoolAssetNonSplittable<PoolUsdAmount> {
    let amount = KrakenAmount::new("ZUSD", amount).unwrap();

    PoolAssetNonSplittable {
        amount: PoolUsdAmount::try_from(amount).unwrap(),
        lifecycle: BasisLifecycle::lifecycle_from_base(),
    }
}

/// The EventInfo of a trade, with an asset-out rate of `EVENT_RATE`.
fn event_info(event_date: DateTime<Utc>) -> EventInfo {
    let mut event_info = EventInfo::new(
        event_date,
        String::new(),
        "event-row".to_string(),
        EventSubType::Trade,
        String::new(),
    );
    event_info.asset_out_exchange_rate = Some(usd(EVENT_RATE));

    event_info
}

/// The gain term of the fee atom that from_fee_split() makes from the split.
fn fee_gain_term<A>(
    split: PoolAssetNonSplittable<A>,
    event_date: DateTime<Utc>,
    fee_rate: i64,
) -> GainTerm
where
    A: Asset + Copy,
    KrakenAmount: From<A>,
{
    let atom = EventAtom::from_fee_split(
        split,
        &event_info(event_date),
        usd(fee_rate),
        &gain_config(),
        &None,
    )
    .unwrap();

    atom.net_gain().unwrap().clone()
}

/// assert_divided() checks the two gain portions of a split of 1 BTC with a basis of 100 USD.  The
/// US portion contains the net gain before the move date.  The bona fide portion contains the net
/// gain after the move date.
fn assert_divided(us: &GainPortion, bona_fide: &GainPortion, basis_date: DateTime<Utc>) {
    assert_eq!(us.basis, usd(100));
    assert_eq!(us.basis_date, basis_date);
    assert_eq!(us.net_gain, usd(MOVE_DATE_RATE - 100));

    assert_eq!(bona_fide.basis, usd(MOVE_DATE_RATE));
    assert_eq!(bona_fide.basis_date, move_date());
    assert_eq!(bona_fide.net_gain, usd(EVENT_RATE - MOVE_DATE_RATE));
}

/// For a BTC fee split, the basis date is before the move date, and the event date is after the
/// move date.  The holding period is less than one year.
#[test]
#[traced_test]
fn fee_split_divided_short_term() {
    let basis_date = date(2024, 1, 16);
    let split = btc_split("1.00000000", basis_date, 100);

    match fee_gain_term(split, date(2024, 8, 5), EVENT_RATE) {
        GainTerm::Short { us, bona_fide } => assert_divided(&us, &bona_fide, basis_date),
        other => panic!("expected GainTerm::Short, got {other:?}"),
    }
}

/// For a BTC fee split, the basis date is before the move date, and the event date is after the
/// move date.  The holding period is more than one year.
#[test]
#[traced_test]
fn fee_split_divided_long_term() {
    let basis_date = date(2023, 1, 16);
    let split = btc_split("1.00000000", basis_date, 100);

    match fee_gain_term(split, date(2024, 8, 5), EVENT_RATE) {
        GainTerm::Long { us, bona_fide } => assert_divided(&us, &bona_fide, basis_date),
        other => panic!("expected GainTerm::Long, got {other:?}"),
    }
}

/// For a BTC fee split, the event date is before the move date.
#[test]
#[traced_test]
fn fee_split_before_move_date() {
    let split = btc_split("1.00000000", date(2024, 1, 16), 100);

    match fee_gain_term(split, date(2024, 2, 1), EVENT_RATE) {
        GainTerm::ShortUs(us) => assert_eq!(us.net_gain, usd(EVENT_RATE - 100)),
        other => panic!("expected GainTerm::ShortUs, got {other:?}"),
    }
}

/// For a BTC fee split, the basis date is after the move date.
#[test]
#[traced_test]
fn fee_split_basis_after_move_date() {
    let split = btc_split("1.00000000", date(2024, 3, 1), 100);

    match fee_gain_term(split, date(2024, 8, 5), EVENT_RATE) {
        GainTerm::ShortBonaFide(bona_fide) => {
            assert_eq!(bona_fide.basis, usd(100));
            assert_eq!(bona_fide.net_gain, usd(EVENT_RATE - 100));
        }
        other => panic!("expected GainTerm::ShortBonaFide, got {other:?}"),
    }
}

/// For a USD fee split, the event date is before the move date.
#[test]
#[traced_test]
fn usd_fee_before_move_date() {
    let split = usd_split("10.0000");

    match fee_gain_term(split, date(2024, 2, 1), 1) {
        GainTerm::ShortUs(us) => assert_eq!(us.net_gain, usd(0)),
        other => panic!("expected GainTerm::ShortUs, got {other:?}"),
    }
}

/// For a USD fee split, the event date is after the move date.
#[test]
#[traced_test]
fn usd_fee_after_move_date() {
    let split = usd_split("10.0000");

    match fee_gain_term(split, date(2024, 8, 5), 1) {
        GainTerm::ShortBonaFide(bona_fide) => assert_eq!(bona_fide.net_gain, usd(0)),
        other => panic!("expected GainTerm::ShortBonaFide, got {other:?}"),
    }
}

/// In one Event, add_trade() and add_fee() each get a BTC split with the dates of
/// fee_split_divided_long_term().  The two event atoms contain the same type of gain term.
#[test]
#[traced_test]
fn trade_and_fee_divided_in_one_event() {
    let gain_config = gain_config();
    let basis_date = date(2023, 1, 16);
    let mut event = Event {
        worksheet_name: Rc::from("test"),
        event_info: event_info(date(2024, 8, 5)),
        is_withdrawal: false,
        event_details: vec![],
    };

    let errors = event.add_trade(vec![btc_split("1.00000000", basis_date, 100)], &gain_config);
    assert!(errors.is_empty(), "{errors:?}");

    let (errors, _) = event.add_fee(
        vec![btc_split("0.01000000", basis_date, 100)],
        Some(usd(EVENT_RATE)),
        &gain_config,
        None,
    );
    assert!(errors.is_empty(), "{errors:?}");

    assert_eq!(event.event_details.len(), 2);
    for atom in &event.event_details {
        match atom.net_gain() {
            Some(GainTerm::Long { bona_fide, .. }) => {
                assert_eq!(bona_fide.basis_date, move_date(), "{} atom", atom.name());
            }
            other => panic!(
                "{} atom: expected GainTerm::Long, got {other:?}",
                atom.name()
            ),
        }
    }
}
