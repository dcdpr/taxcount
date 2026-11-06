// 04 / Bona Fide Move-Date Value is required in ledgers-sums for reconciliation
// issue-004-sums-fees.md
// Section 5, Tests

use super::*;
use crate::model::kraken_amount::{BitcoinAmount, KrakenAmount};
use chrono::TimeZone;

fn usd(n: i64) -> UsdAmount {
    UsdAmount::from_int(n)
}

fn btc(s: &str) -> KrakenAmount {
    KrakenAmount::new("XXBT", s).unwrap()
}

fn date(year: i32, month: u32, day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).unwrap()
}

/// A gain portion with the given basis and net gain.
fn portion(basis: i64, net_gain: i64, basis_date: DateTime<Utc>) -> GainPortion {
    GainPortion {
        basis: usd(basis),
        basis_date,
        basis_synthetic_id: String::new(),
        net_gain: usd(net_gain),
    }
}

/// A worksheet with one event.
fn worksheet(
    event_subtype: EventSubType,
    proceeds: UsdAmount,
    event_details: Vec<EventAtom>,
) -> CapGainsWorksheet {
    CapGainsWorksheet {
        worksheet: vec![CapGainsWorksheetRow {
            event_date: date(2024, 6, 1),
            internal_account: String::new(),
            ledger_row_id: String::new(),
            event_subtype,
            event_name: String::new(),
            asset_out_exchange_rate: String::new(),
            asset_in_exchange_rate: String::new(),
            proceeds,
            event_details,
        }],
    }
}

/// Regression test for issue #3: pr_statement24_dates() must collect dates from
/// fee atoms in `event_details`, not only trade atoms. A worksheet with fee-only
/// bona fide activity must still produce dates for the PR Statement-24 row.
#[test]
fn pr_statement24_dates_from_fees_only() {
    let basis_date = date(2024, 6, 1);
    let event_date = date(2024, 12, 1);

    let worksheet = CapGainsWorksheet {
        worksheet: vec![CapGainsWorksheetRow {
            event_date,
            internal_account: String::new(),
            ledger_row_id: String::new(),
            event_subtype: EventSubType::Trade,
            event_name: String::new(),
            asset_out_exchange_rate: String::new(),
            asset_in_exchange_rate: String::new(),
            proceeds: usd(0),
            event_details: vec![EventAtom::Fee {
                asset_amount: KrakenAmount::Btc(BitcoinAmount::default()),
                proceeds: usd(0),
                net_gain: GainTerm::ShortBonaFide(portion(5000, -5000, basis_date)),
                ledger_row_id: None,
            }],
        }],
    };

    let dates = worksheet.pr_statement24_dates();

    assert_eq!(dates.st_earliest_acquired, Some(basis_date));
    assert_eq!(dates.st_latest_sold, Some(event_date));
    assert_eq!(dates.lt_earliest_acquired, None);
    assert_eq!(dates.lt_latest_sold, None);
}

/// A split that the move date divides: the US cell gets the basis, the move-date value, and the
/// gain up to the move date. The Non-US cell gets the proceeds and the move-date value as basis.
/// The identity holds in each cell.
#[test]
fn test_regression_bona_fide_proceeds() {
    let worksheet = worksheet(
        EventSubType::Trade,
        usd(1000),
        vec![EventAtom::Trade {
            asset_amount: btc("-1.00000000"),
            proceeds: usd(1000),
            net_gain: GainTerm::Short {
                us: portion(100, 200, date(2020, 1, 1)),
                bona_fide: portion(300, 700, date(2024, 2, 24)),
            },
        }],
    );

    let sums = worksheet.sums();
    sums.assert_error_check();

    let bona_fide = sums
        .gain_matrix
        .bona_fide_short
        .expect("bona fide short-term cell");
    assert_eq!(bona_fide.trade_proceeds, usd(1000));
    assert_eq!(bona_fide.move_date_value, usd(0));
    assert_eq!(bona_fide.trade_basis, usd(300));
    assert_eq!(bona_fide.gain(), usd(700));

    let us = &sums.gain_matrix.us_short;
    assert_eq!(us.trade_proceeds, usd(0));
    assert_eq!(us.move_date_value, usd(300));
    assert_eq!(us.trade_basis, usd(100));
    assert_eq!(us.gain(), usd(200));
}

/// A trade with a long-term lot and a fee paid from a short-term lot. The fee value is an
/// expense of the long-term sale. The fee coin disposal is a short-term gain.
#[test]
fn trade_fee_cost_in_sale_cell() {
    let worksheet = worksheet(
        EventSubType::Trade,
        usd(1000),
        vec![
            EventAtom::Trade {
                asset_amount: btc("-1.00000000"),
                proceeds: usd(1000),
                net_gain: GainTerm::LongUs(portion(10, 990, date(2020, 5, 15))),
            },
            EventAtom::Fee {
                asset_amount: btc("-0.00800000"),
                proceeds: usd(8),
                net_gain: GainTerm::ShortUs(portion(6, 2, date(2024, 1, 10))),
                ledger_row_id: None,
            },
        ],
    );

    let sums = worksheet.sums();
    sums.assert_error_check();

    let us_long = &sums.gain_matrix.us_long;
    assert_eq!(us_long.trade_proceeds, usd(1000));
    assert_eq!(us_long.trade_basis, usd(10));
    assert_eq!(us_long.fees, usd(8));
    assert_eq!(us_long.gain(), usd(982));

    let us_short = &sums.gain_matrix.us_short;
    assert_eq!(us_short.trade_proceeds, usd(8));
    assert_eq!(us_short.trade_basis, usd(6));
    assert_eq!(us_short.fees, usd(0));
    assert_eq!(us_short.gain(), usd(2));

    let details = worksheet.event_details().expect("details");
    let shares: Vec<_> = details
        .details
        .iter()
        .map(|(_, _, fee_share)| *fee_share)
        .collect();
    assert_eq!(shares, vec![Some(usd(8)), None]);
}

/// A trade with two lots. The fee value divides across the lots pro-rata by amount.
#[test]
fn trade_fee_cost_pro_rata_across_lots() {
    let worksheet = worksheet(
        EventSubType::Trade,
        usd(15000),
        vec![
            EventAtom::Trade {
                asset_amount: btc("-0.06000000"),
                proceeds: usd(6000),
                net_gain: GainTerm::LongUs(portion(6, 5994, date(2019, 9, 8))),
            },
            EventAtom::Trade {
                asset_amount: btc("-0.09000000"),
                proceeds: usd(9000),
                net_gain: GainTerm::ShortUs(portion(18, 8982, date(2024, 3, 1))),
            },
            EventAtom::Fee {
                asset_amount: btc("-0.00300000"),
                proceeds: usd(300),
                net_gain: GainTerm::ShortUs(portion(1, 299, date(2024, 3, 1))),
                ledger_row_id: None,
            },
        ],
    );

    let shares = fee_shares(&worksheet.worksheet[0].event_details);
    assert_eq!(shares, vec![Some(usd(120)), Some(usd(180)), None]);

    let sums = worksheet.sums();
    sums.assert_error_check();
    assert_eq!(sums.gain_matrix.us_long.fees, usd(120));
    assert_eq!(sums.gain_matrix.us_long.gain(), usd(5874));
    assert_eq!(sums.gain_matrix.us_short.fees, usd(180));
    assert_eq!(sums.gain_matrix.us_short.gain(), usd(8982 - 180 + 299));
}

/// A margin close with a fee. The fee value is an expense of the position, in the cell of the
/// position atom.
#[test]
fn margin_close_fee_cost_in_position_cell() {
    let worksheet = worksheet(
        EventSubType::MarginClose,
        usd(500),
        vec![
            EventAtom::Position {
                asset_amount: KrakenAmount::new("ZUSD", "500.0000").unwrap(),
                proceeds_us: usd(0),
                proceeds_bona_fide: Some(usd(500)),
            },
            EventAtom::Fee {
                asset_amount: btc("-0.00100000"),
                proceeds: usd(20),
                net_gain: GainTerm::LongUs(portion(1, 19, date(2020, 5, 15))),
                ledger_row_id: None,
            },
        ],
    );

    let sums = worksheet.sums();
    sums.assert_error_check();

    let bona_fide = sums
        .gain_matrix
        .bona_fide_short
        .expect("bona fide short-term cell");
    assert_eq!(bona_fide.position_proceeds, usd(500));
    assert_eq!(bona_fide.fees, usd(20));
    assert_eq!(bona_fide.gain(), usd(480));

    let us_long = &sums.gain_matrix.us_long;
    assert_eq!(us_long.trade_proceeds, usd(20));
    assert_eq!(us_long.trade_basis, usd(1));
    assert_eq!(us_long.gain(), usd(19));
}

/// A withdrawal with a fee. The event is not a sale, so the fee coin disposal is the only gain.
#[test]
fn withdrawal_fee_is_disposal_only() {
    let worksheet = worksheet(
        EventSubType::Withdrawal,
        usd(0),
        vec![EventAtom::Fee {
            asset_amount: btc("-0.00100000"),
            proceeds: usd(20),
            net_gain: GainTerm::LongUs(portion(1, 19, date(2020, 5, 15))),
            ledger_row_id: None,
        }],
    );

    assert_eq!(
        fee_shares(&worksheet.worksheet[0].event_details),
        vec![None]
    );

    let sums = worksheet.sums();
    sums.assert_error_check();

    let us_long = &sums.gain_matrix.us_long;
    assert_eq!(us_long.trade_proceeds, usd(20));
    assert_eq!(us_long.trade_basis, usd(1));
    assert_eq!(us_long.fees, usd(0));
    assert_eq!(us_long.gain(), usd(19));
}

/// A rollover fee is an investment fee atom: a fee coin disposal, and not an expense of a sale.
#[test]
fn rollover_fee_is_disposal_only() {
    let worksheet = worksheet(
        EventSubType::MarginRollover,
        usd(0),
        vec![EventAtom::InvestmentFee {
            asset_amount: btc("-0.00100000"),
            proceeds: usd(20),
            net_gain: GainTerm::LongUs(portion(1, 19, date(2020, 5, 15))),
            ledger_row_id: None,
        }],
    );

    let sums = worksheet.sums();
    sums.assert_error_check();

    let us_long = &sums.gain_matrix.us_long;
    assert_eq!(us_long.trade_proceeds, usd(20));
    assert_eq!(us_long.fees, usd(0));
    assert_eq!(us_long.gain(), usd(19));
}

#[test]
fn sums_csv_rows() {
    let worksheet = worksheet(
        EventSubType::Trade,
        usd(1000),
        vec![
            EventAtom::Trade {
                asset_amount: btc("-1.00000000"),
                proceeds: usd(1000),
                net_gain: GainTerm::Short {
                    us: portion(100, 200, date(2020, 1, 1)),
                    bona_fide: portion(300, 700, date(2024, 2, 24)),
                },
            },
            EventAtom::Fee {
                asset_amount: btc("-0.00800000"),
                proceeds: usd(8),
                net_gain: GainTerm::ShortUs(portion(6, 2, date(2024, 1, 10))),
                ledger_row_id: None,
            },
        ],
    );

    let expected = concat!(
        r#""","US Long Term","US Short Term","Non-US Long Term","Non-US Short Term""#,
        "\n",
        r#""Trade Proceeds","0.0000","8.0000","","1000.0000""#,
        "\n",
        r#""Move-Date Value","0.0000","300.0000","","""#,
        "\n",
        r#""Trade Basis","0.0000","106.0000","","300.0000""#,
        "\n",
        r#""Position Proceeds","","0.0000","","0.0000""#,
        "\n",
        r#""Fees","0.0000","0.0000","","8.0000""#,
        "\n",
        r#""Gain","0.0000","202.0000","","692.0000""#,
        "\n",
    );
    assert_eq!(worksheet.sums().to_string(), expected);
}

#[test]
fn pr_statement24_from_sums() {
    let sums = Sums {
        ledger_proceeds: usd(0),
        gain_matrix: GainMatrix {
            event_proceeds: usd(0),
            us_short: GainCell {
                trade_proceeds: usd(50),
                ..Default::default()
            },
            us_long: GainCell {
                trade_proceeds: usd(200),
                ..Default::default()
            },
            bona_fide_short: Some(GainCell {
                trade_proceeds: usd(500),
                trade_basis: usd(400),
                ..Default::default()
            }),
            bona_fide_long: Some(GainCell {
                trade_proceeds: usd(1000),
                trade_basis: usd(700),
                ..Default::default()
            }),
        },
    };

    let dates = PrStatement24Dates {
        lt_earliest_acquired: Some(date(2020, 1, 15)),
        lt_latest_sold: Some(date(2024, 12, 15)),
        st_earliest_acquired: Some(date(2024, 3, 1)),
        st_latest_sold: Some(date(2024, 11, 20)),
    };

    let worksheet_name = WorksheetName::from("test-exchange".to_string());
    let statement = PrStatement24::from_worksheet(&worksheet_name, &sums, &dates);

    let expected = concat!(
        r#""Property Description","Date Acquired","Date Sold","A: Sale Price","B: Market Value","C: Adjusted Basis","D: Gain or Loss","E: US-Sourced Gain","F: PR-Sourced Gain""#,
        "\n",
        r#""investment assets test-exchange LT","2020-01-15","2024-12-15","1000.0000","700.0000","500.0000","500.0000","200.0000","300.0000""#,
        "\n",
        r#""investment assets test-exchange ST","2024-03-01","2024-11-20","500.0000","400.0000","350.0000","150.0000","50.0000","100.0000""#,
        "\n",
    );
    assert_eq!(statement.to_string(), expected);
}
