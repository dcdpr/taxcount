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

/// A CapGainsWorksheetRow for one event.
fn row(
    event_date: DateTime<Utc>,
    event_subtype: EventSubType,
    event_details: Vec<EventAtom>,
) -> CapGainsWorksheetRow {
    CapGainsWorksheetRow {
        event_date,
        internal_account: String::new(),
        ledger_row_id: String::new(),
        event_subtype,
        event_name: String::new(),
        asset_out_exchange_rate: String::new(),
        asset_in_exchange_rate: String::new(),
        event_details,
    }
}

/// A worksheet with one event.
fn worksheet(event_subtype: EventSubType, event_details: Vec<EventAtom>) -> CapGainsWorksheet {
    CapGainsWorksheet {
        worksheet: vec![row(date(2024, 6, 1), event_subtype, event_details)],
    }
}

/// The header line of PR Statement 24.
const PR_STATEMENT24_HEADER: &str = concat!(
    r#""Property Description","Date Acquired","Date Sold","A: Sale Price","#,
    r#""B: Market Value","C: Adjusted Basis","D: Gain or Loss","E: US-Sourced Gain","#,
    r#""F: PR-Sourced Gain""#,
    "\n",
);

/// The CapGainsWorksheet contains one fee atom with a bona fide portion only.  PR Statement 24
/// contains a row with the dates of that fee atom.  See #3 and #5.
#[test]
fn pr_statement24_dates_from_fees_only() {
    let basis_date = date(2024, 6, 1);
    let event_date = date(2024, 12, 1);

    let worksheet = CapGainsWorksheet {
        worksheet: vec![row(
            event_date,
            EventSubType::Trade,
            vec![EventAtom::Fee {
                asset_amount: KrakenAmount::Btc(BitcoinAmount::default()),
                proceeds: usd(0),
                net_gain: GainTerm::ShortBonaFide(portion(5000, -5000, basis_date)),
                ledger_row_id: None,
            }],
        )],
    };

    let worksheet_name = WorksheetName::from("test-exchange".to_string());
    let statement = PrStatement24::from_worksheet(&worksheet_name, &worksheet);

    let expected = format!(
        "{PR_STATEMENT24_HEADER}{}\n",
        concat!(
            r#""investment assets test-exchange ST","2024-06-01","2024-12-01","0.0000","#,
            r#""5000.0000","5000.0000","-5000.0000","0.0000","-5000.0000""#,
        ),
    );
    assert_eq!(statement.to_string(), expected);
}

/// A split that the move date divides: the US cell gets the basis, the move-date value, and the
/// gain up to the move date. The Non-US cell gets the proceeds and the move-date value as basis.
/// The identity holds in each cell.
#[test]
fn test_regression_bona_fide_proceeds() {
    let worksheet = worksheet(
        EventSubType::Trade,
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

/// A fee atom with two gain portions.  The US gain cell contains the basis, the move-date value,
/// and the net gain before the move date.  The Non-US gain cell contains the proceeds, and the
/// move-date value as its basis.  See #10.
#[test]
fn fee_divided_by_move_date() {
    let worksheet = worksheet(
        EventSubType::Withdrawal,
        vec![EventAtom::Fee {
            asset_amount: btc("-0.01000000"),
            proceeds: usd(10),
            net_gain: GainTerm::Long {
                us: portion(1, 2, date(2023, 1, 16)),
                bona_fide: portion(3, 7, date(2024, 2, 24)),
            },
            ledger_row_id: None,
        }],
    );

    let sums = worksheet.sums();
    sums.assert_error_check();

    let us_long = &sums.gain_matrix.us_long;
    assert_eq!(us_long.trade_proceeds, usd(0));
    assert_eq!(us_long.move_date_value, usd(3));
    assert_eq!(us_long.trade_basis, usd(1));
    assert_eq!(us_long.gain(), usd(2));

    let bona_fide = sums
        .gain_matrix
        .bona_fide_long
        .expect("bona fide long-term cell");
    assert_eq!(bona_fide.trade_proceeds, usd(10));
    assert_eq!(bona_fide.move_date_value, usd(0));
    assert_eq!(bona_fide.trade_basis, usd(3));
    assert_eq!(bona_fide.gain(), usd(7));
}

/// A trade with a long-term lot and a fee paid from a short-term lot. The fee value is an
/// expense of the long-term sale. The fee coin disposal is a short-term gain.
#[test]
fn trade_fee_cost_in_sale_cell() {
    let worksheet = worksheet(
        EventSubType::Trade,
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

/// The event details file contains a line for the position atom of a margin close.  On that line,
/// the "Proceeds" column contains the proceeds, and the "Fees" column contains the fee share.
#[test]
fn margin_close_proceeds_in_event_details() {
    let worksheet = worksheet(
        EventSubType::MarginClose,
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

    let details = worksheet.event_details().expect("details").to_string();
    let position = details
        .lines()
        .find(|line| line.contains(r#""Position""#))
        .expect("position line");
    assert!(
        position.starts_with(r#""","Position","USD","500.0000","500.0000","20.0000","#),
        "{position}"
    );
}

/// For a position atom, proceeds() returns the sum of `proceeds_us` and `proceeds_bona_fide`.
#[test]
fn position_proceeds_us_and_bona_fide() {
    let us = EventAtom::Position {
        asset_amount: KrakenAmount::new("ZUSD", "12.0000").unwrap(),
        proceeds_us: usd(12),
        proceeds_bona_fide: None,
    };
    assert_eq!(us.proceeds(), usd(12));

    let bona_fide = EventAtom::Position {
        asset_amount: KrakenAmount::new("ZUSD", "34.0000").unwrap(),
        proceeds_us: usd(0),
        proceeds_bona_fide: Some(usd(34)),
    };
    assert_eq!(bona_fide.proceeds(), usd(34));
}

/// A withdrawal with a fee. The event is not a sale, so the fee coin disposal is the only gain.
#[test]
fn withdrawal_fee_is_disposal_only() {
    let worksheet = worksheet(
        EventSubType::Withdrawal,
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

/// The "Proceeds" column of the worksheet CSV is the sum of the trade, income, and position
/// atoms. The fee atoms and the investment fee atoms are disposals of the fee coin, and the
/// column does not include them.
#[test]
fn worksheet_proceeds_column_from_atoms() {
    let worksheet = worksheet(
        EventSubType::Trade,
        vec![
            EventAtom::Trade {
                asset_amount: btc("-1.00000000"),
                proceeds: usd(1000),
                net_gain: GainTerm::LongUs(portion(10, 990, date(2020, 5, 15))),
            },
            EventAtom::Income {
                asset_amount: btc("0.05000000"),
                proceeds: usd(50),
            },
            EventAtom::Position {
                asset_amount: KrakenAmount::new("ZUSD", "500.0000").unwrap(),
                proceeds_us: usd(0),
                proceeds_bona_fide: Some(usd(500)),
            },
            EventAtom::Fee {
                asset_amount: btc("-0.00800000"),
                proceeds: usd(8),
                net_gain: GainTerm::ShortUs(portion(6, 2, date(2024, 1, 10))),
                ledger_row_id: None,
            },
            EventAtom::InvestmentFee {
                asset_amount: btc("-0.00300000"),
                proceeds: usd(3),
                net_gain: GainTerm::ShortUs(portion(1, 2, date(2024, 1, 10))),
                ledger_row_id: None,
            },
        ],
    );

    let row = worksheet.worksheet[0].to_string();
    assert!(row.ends_with(r#","1550.0000""#), "{row}");
}

#[test]
fn sums_csv_rows() {
    let worksheet = worksheet(
        EventSubType::Trade,
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

/// PR Statement 24 contains each split with a bona fide portion, and each position atom with
/// proceeds_bona_fide.  Column A contains the proceeds minus the fee shares.  Column B contains
/// the basis of each bona fide portion.  Column C contains the basis of each US portion, and the
/// basis of each split with a bona fide portion only.  The trade of the split with a US portion
/// only is not in a row.  See #5.
#[test]
fn pr_statement24_from_atoms() {
    let move_date = date(2024, 2, 24);
    let worksheet = CapGainsWorksheet {
        worksheet: vec![
            row(
                date(2024, 1, 10),
                EventSubType::Trade,
                vec![EventAtom::Trade {
                    asset_amount: btc("-0.10000000"),
                    proceeds: usd(100),
                    net_gain: GainTerm::ShortUs(portion(40, 60, date(2023, 12, 1))),
                }],
            ),
            row(
                date(2024, 6, 1),
                EventSubType::Trade,
                vec![
                    EventAtom::Trade {
                        asset_amount: btc("-1.00000000"),
                        proceeds: usd(1000),
                        net_gain: GainTerm::Short {
                            us: portion(100, 200, date(2024, 1, 5)),
                            bona_fide: portion(300, 700, move_date),
                        },
                    },
                    EventAtom::Fee {
                        asset_amount: btc("-0.00800000"),
                        proceeds: usd(8),
                        net_gain: GainTerm::Short {
                            us: portion(1, 2, date(2024, 1, 5)),
                            bona_fide: portion(3, 5, move_date),
                        },
                        ledger_row_id: None,
                    },
                ],
            ),
            row(
                date(2024, 7, 1),
                EventSubType::Trade,
                vec![EventAtom::Trade {
                    asset_amount: btc("-0.50000000"),
                    proceeds: usd(500),
                    net_gain: GainTerm::ShortBonaFide(portion(400, 100, date(2024, 3, 1))),
                }],
            ),
            row(
                date(2024, 8, 1),
                EventSubType::MarginClose,
                vec![
                    EventAtom::Position {
                        asset_amount: KrakenAmount::new("ZUSD", "50.0000").unwrap(),
                        proceeds_us: usd(0),
                        proceeds_bona_fide: Some(usd(50)),
                    },
                    EventAtom::Fee {
                        asset_amount: btc("-0.00500000"),
                        proceeds: usd(5),
                        net_gain: GainTerm::ShortBonaFide(portion(4, 1, date(2024, 3, 1))),
                        ledger_row_id: None,
                    },
                ],
            ),
            row(
                date(2024, 9, 1),
                EventSubType::MarginRollover,
                vec![EventAtom::InvestmentFee {
                    asset_amount: btc("-0.00020000"),
                    proceeds: usd(20),
                    net_gain: GainTerm::Long {
                        us: portion(2, 8, date(2022, 5, 15)),
                        bona_fide: portion(10, 10, move_date),
                    },
                    ledger_row_id: None,
                }],
            ),
        ],
    };

    let worksheet_name = WorksheetName::from("test-exchange".to_string());
    let statement = PrStatement24::from_worksheet(&worksheet_name, &worksheet);

    let expected = format!(
        "{PR_STATEMENT24_HEADER}{}\n{}\n",
        concat!(
            r#""investment assets test-exchange LT","2022-05-15","2024-09-01","20.0000","#,
            r#""10.0000","2.0000","18.0000","8.0000","10.0000""#,
        ),
        concat!(
            r#""investment assets test-exchange ST","2024-01-05","2024-08-01","1550.0000","#,
            r#""707.0000","505.0000","1045.0000","202.0000","843.0000""#,
        ),
    );
    assert_eq!(statement.to_string(), expected);

    // F is the capital gain of the Non-US gain cell in the sums table.
    let sums = worksheet.sums();
    sums.assert_error_check();
    let bona_fide_short = sums
        .gain_matrix
        .bona_fide_short
        .expect("bona fide short cell");
    let bona_fide_long = sums
        .gain_matrix
        .bona_fide_long
        .expect("bona fide long cell");
    assert_eq!(bona_fide_short.gain(), usd(843));
    assert_eq!(bona_fide_long.gain(), usd(10));
}

/// A split with a US portion only is not PR property.  For a CapGainsWorksheet with only such a
/// split, PR Statement 24 contains only the header line.  See #5.
#[test]
fn pr_statement24_without_pre_move_splits() {
    let worksheet = worksheet(
        EventSubType::Trade,
        vec![EventAtom::Trade {
            asset_amount: btc("-0.10000000"),
            proceeds: usd(100),
            net_gain: GainTerm::ShortUs(portion(40, 60, date(2023, 12, 1))),
        }],
    );

    let worksheet_name = WorksheetName::from("test-exchange".to_string());
    let statement = PrStatement24::from_worksheet(&worksheet_name, &worksheet);

    assert_eq!(statement.to_string(), PR_STATEMENT24_HEADER);
}
