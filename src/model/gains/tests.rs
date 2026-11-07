use super::*;
use crate::model::kraken_amount::{BitcoinAmount, KrakenAmount};
use chrono::TimeZone;

fn usd(n: i64) -> UsdAmount {
    UsdAmount::from_int(n)
}

/// Regression test for issue #3: pr_statement24_dates() must collect dates from
/// fee atoms in `event_details`, not only trade atoms. A worksheet with fee-only
/// bona fide activity must still produce dates for the PR Statement-24 row.
#[test]
fn pr_statement24_dates_from_fees_only() {
    let basis_date = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
    let event_date = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap();

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
                net_gain: GainTerm::ShortBonaFide(GainPortion {
                    basis: usd(5000),
                    basis_date,
                    basis_synthetic_id: String::new(),
                    net_gain: usd(-5000),
                }),
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

#[test]
fn test_regression_bona_fide_proceeds() {
    let worksheet = CapGainsWorksheet {
        worksheet: vec![CapGainsWorksheetRow {
            event_date: Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap(),
            internal_account: String::new(),
            ledger_row_id: String::new(),
            event_subtype: EventSubType::Trade,
            event_name: String::new(),
            asset_out_exchange_rate: String::new(),
            asset_in_exchange_rate: String::new(),
            proceeds: usd(1000),
            event_details: vec![EventAtom::Trade {
                asset_amount: KrakenAmount::Btc(BitcoinAmount::default()),
                proceeds: usd(1000),
                net_gain: GainTerm::Short {
                    us: GainPortion {
                        basis: usd(100),
                        basis_date: Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
                        basis_synthetic_id: String::new(),
                        net_gain: usd(200),
                    },
                    bona_fide: GainPortion {
                        basis: usd(300),
                        basis_date: Utc.with_ymd_and_hms(2024, 2, 24, 0, 0, 0).unwrap(),
                        basis_synthetic_id: String::new(),
                        net_gain: usd(700),
                    },
                },
            }],
        }],
    };

    let sums = worksheet.sums();

    let bona_fide = sums
        .gain_matrix
        .bona_fide_short
        .expect("bona fide short-term cell");
    assert_eq!(bona_fide.trade_proceeds, usd(1000));
    assert_eq!(bona_fide.trade_basis, usd(300));
    assert_eq!(sums.gain_matrix.us_short.trade_proceeds, usd(0));
    assert_eq!(sums.gain_matrix.us_short.trade_basis, usd(100));
}

#[test]
fn sums_move_date_value() {
    let trade = EventAtom::Trade {
        asset_amount: KrakenAmount::Btc(BitcoinAmount::default()),
        proceeds: usd(1000),
        net_gain: GainTerm::Short {
            us: GainPortion {
                basis: usd(100),
                basis_date: Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
                basis_synthetic_id: String::new(),
                net_gain: usd(200),
            },
            bona_fide: GainPortion {
                basis: usd(300),
                basis_date: Utc.with_ymd_and_hms(2024, 2, 24, 0, 0, 0).unwrap(),
                basis_synthetic_id: "Special election for bona fide residents".to_string(),
                net_gain: usd(700),
            },
        },
    };
    let trade_fee = EventAtom::TradeFee {
        asset_amount: KrakenAmount::Btc(BitcoinAmount::default()),
        proceeds: usd(2),
        net_gain: GainTerm::ShortUs(GainPortion {
            basis: usd(1),
            basis_date: Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
            basis_synthetic_id: String::new(),
            net_gain: usd(1),
        }),
        ledger_row_id: None,
    };
    let withdrawal_fee = EventAtom::Fee {
        asset_amount: KrakenAmount::Btc(BitcoinAmount::default()),
        proceeds: usd(3),
        net_gain: GainTerm::ShortUs(GainPortion {
            basis: usd(5),
            basis_date: Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
            basis_synthetic_id: String::new(),
            net_gain: usd(-2),
        }),
        ledger_row_id: None,
    };

    let worksheet = CapGainsWorksheet {
        worksheet: vec![
            CapGainsWorksheetRow {
                event_date: Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap(),
                internal_account: String::new(),
                ledger_row_id: String::new(),
                event_subtype: EventSubType::Trade,
                event_name: String::new(),
                asset_out_exchange_rate: String::new(),
                asset_in_exchange_rate: String::new(),
                proceeds: usd(1002),
                event_details: vec![trade, trade_fee],
            },
            CapGainsWorksheetRow {
                event_date: Utc.with_ymd_and_hms(2024, 12, 2, 0, 0, 0).unwrap(),
                internal_account: String::new(),
                ledger_row_id: String::new(),
                event_subtype: EventSubType::Withdrawal,
                event_name: String::new(),
                asset_out_exchange_rate: String::new(),
                asset_in_exchange_rate: String::new(),
                proceeds: usd(0),
                event_details: vec![withdrawal_fee],
            },
        ],
    };

    let sums = worksheet.sums();
    sums.assert_error_check();

    assert_eq!(sums.gain_matrix.income, usd(0));

    let us_long = &sums.gain_matrix.us_long;
    assert_eq!(us_long.trade_basis, usd(0));
    assert_eq!(us_long.trade_proceeds, usd(0));
    assert_eq!(us_long.trade_fee_proceeds, usd(0));
    assert_eq!(us_long.fee_proceeds, usd(0));
    assert_eq!(us_long.move_date_value, usd(0));
    assert_eq!(us_long.position_fees, usd(0));
    assert_eq!(us_long.trade_gain, usd(0));

    let us_short = &sums.gain_matrix.us_short;
    assert_eq!(us_short.trade_basis, usd(106));
    assert_eq!(us_short.trade_proceeds, usd(0));
    assert_eq!(us_short.trade_fee_proceeds, usd(2));
    assert_eq!(us_short.fee_proceeds, usd(3));
    assert_eq!(us_short.move_date_value, usd(300));
    assert_eq!(us_short.position_proceeds, usd(0));
    assert_eq!(us_short.position_fees, usd(0));
    assert_eq!(us_short.trade_gain, usd(199));

    assert!(sums.gain_matrix.bona_fide_long.is_none());

    let bona_fide_short = sums
        .gain_matrix
        .bona_fide_short
        .as_ref()
        .expect("bona fide short-term cell");
    assert_eq!(bona_fide_short.trade_basis, usd(300));
    assert_eq!(bona_fide_short.trade_proceeds, usd(1000));
    assert_eq!(bona_fide_short.trade_fee_proceeds, usd(0));
    assert_eq!(bona_fide_short.fee_proceeds, usd(0));
    assert_eq!(bona_fide_short.move_date_value, usd(0));
    assert_eq!(bona_fide_short.position_proceeds, usd(0));
    assert_eq!(bona_fide_short.position_fees, usd(0));
    assert_eq!(bona_fide_short.trade_gain, usd(700));

    assert_eq!(sums.gains_us_long, usd(0));
    assert_eq!(sums.gains_us_short, usd(199));
    assert_eq!(sums.gains_bona_fide_long, None);
    assert_eq!(sums.gains_bona_fide_short, Some(usd(700)));

    assert_eq!(sums.ledger_proceeds(), usd(1002));

    // No investment interest expense in this worksheet.
    assert!(Form4952::new(&sums).is_none());
}

#[test]
fn form_4952_worksheet() {
    let mut gain_matrix = GainMatrix::default();
    gain_matrix.us_long.trade_gain = usd(120);
    gain_matrix.us_long.position_fees = usd(100);
    gain_matrix.us_short.trade_gain = usd(30);
    gain_matrix.us_short.position_proceeds = usd(20);
    gain_matrix.us_short.position_fees = usd(50);
    gain_matrix.bona_fide_short = Some(GainMatrixShort {
        trade_gain: usd(5),
        position_fees: usd(10),
        ..Default::default()
    });

    let sums = Sums {
        ledger_proceeds: usd(0),
        gain_matrix,
        gains_us_short: usd(50), // 30 (trade) + 20 (position proceeds)
        gains_us_long: usd(120),
        gains_bona_fide_short: Some(usd(5)),
        gains_bona_fide_long: None,
    };

    let form = Form4952::new(&sums).expect("investment interest expense");

    let us_long = form.us_long;
    assert_eq!(us_long.expense, usd(100));
    assert_eq!(us_long.limited, usd(100));
    assert_eq!(us_long.carryover, usd(0));

    let us_short = form.us_short;
    assert_eq!(us_short.expense, usd(50));
    assert_eq!(us_short.limited, usd(50));
    assert_eq!(us_short.carryover, usd(0));

    assert_eq!(form.bona_fide_long, None);

    let bona_fide_short = form.bona_fide_short.expect("bona fide short-term expense");
    assert_eq!(bona_fide_short.expense, usd(10));
    assert_eq!(bona_fide_short.limited, usd(5));
    assert_eq!(bona_fide_short.carryover, usd(5));

    // A worksheet without any investment interest expense reports None and leaves the total
    // unchanged.
    let empty_sums = Sums {
        ledger_proceeds: usd(0),
        gain_matrix: GainMatrix::default(),
        gains_us_short: usd(0),
        gains_us_long: usd(0),
        gains_bona_fide_short: None,
        gains_bona_fide_long: None,
    };
    assert!(Form4952::new(&empty_sums).is_none());

    // A second worksheet: a US long-term expense that exceeds the gain (carryover), and a
    // Non-US long-term expense that adds a column to the total.
    let mut gain_matrix2 = GainMatrix::default();
    gain_matrix2.us_long.trade_gain = usd(10);
    gain_matrix2.us_long.position_fees = usd(20);
    gain_matrix2.bona_fide_long = Some(GainMatrixLong {
        trade_gain: usd(8),
        position_fees: usd(5),
        ..Default::default()
    });
    let sums2 = Sums {
        ledger_proceeds: usd(0),
        gain_matrix: gain_matrix2,
        gains_us_short: usd(0),
        gains_us_long: usd(10),
        gains_bona_fide_short: None,
        gains_bona_fide_long: Some(usd(8)),
    };

    // The first expense worksheet starts the total; later worksheets sum into it.
    let mut total: Option<Form4952> = None;
    Form4952::accumulate(&mut total, &empty_sums);
    assert!(total.is_none());
    Form4952::accumulate(&mut total, &sums);
    Form4952::accumulate(&mut total, &sums2);
    let total = total.expect("tax-year total");

    let us_long = total.us_long;
    assert_eq!(us_long.expense, usd(120)); // 100 + 20
    assert_eq!(us_long.limited, usd(110)); // 100 + 10
    assert_eq!(us_long.carryover, usd(10)); // 0 + 10
    let us_short = total.us_short;
    assert_eq!(us_short.expense, usd(50));
    assert_eq!(us_short.limited, usd(50));
    assert_eq!(us_short.carryover, usd(0));
    let bona_fide_long = total.bona_fide_long.expect("bona fide long-term expense");
    assert_eq!(bona_fide_long.expense, usd(5));
    assert_eq!(bona_fide_long.limited, usd(5));
    assert_eq!(bona_fide_long.carryover, usd(0));
    let bona_fide_short = total.bona_fide_short.expect("bona fide short-term expense");
    assert_eq!(bona_fide_short.expense, usd(10));
    assert_eq!(bona_fide_short.limited, usd(5));
    assert_eq!(bona_fide_short.carryover, usd(5));
}

#[test]
fn pr_statement24_from_sums() {
    let sums = Sums {
        ledger_proceeds: usd(0),
        gain_matrix: GainMatrix {
            income: usd(0),
            us_short: GainMatrixShort::default(),
            us_long: GainMatrixLong::default(),
            bona_fide_short: Some(GainMatrixShort {
                trade_proceeds: usd(500),
                ..Default::default()
            }),
            bona_fide_long: Some(GainMatrixLong {
                trade_proceeds: usd(1000),
                ..Default::default()
            }),
        },
        gains_us_short: usd(50),
        gains_us_long: usd(200),
        gains_bona_fide_short: Some(usd(100)),
        gains_bona_fide_long: Some(usd(300)),
    };

    let dates = PrStatement24Dates {
        lt_earliest_acquired: Some(Utc.with_ymd_and_hms(2020, 1, 15, 0, 0, 0).unwrap()),
        lt_latest_sold: Some(Utc.with_ymd_and_hms(2024, 12, 15, 0, 0, 0).unwrap()),
        st_earliest_acquired: Some(Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap()),
        st_latest_sold: Some(Utc.with_ymd_and_hms(2024, 11, 20, 0, 0, 0).unwrap()),
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
