use crate::basis::{Asset, BasisLifecycle, Bucket, PoolAsset};
use crate::imports::kraken::{BasisCSVRow, LedgerCSVRow, TradeCSVRow};
use crate::model::kraken_amount::{FiatAmount, KrakenAmount, UsdAmount};
use crate::util::year_ext::GetYear;
use chrono::{DateTime, Datelike as _, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BasisRow {
    pub synthetic_id: String,
    pub time: DateTime<Utc>,
    pub asset: String,
    pub amount: Option<KrakenAmount>,
    pub exchange_rate: FiatAmount,
}

/// Enable consistency checks on years.
impl GetYear for BasisRow {
    fn get_year(&self) -> i32 {
        self.time.year()
    }
}

pub(crate) fn basis_lookup_parse(r: BasisCSVRow) -> BasisRow {
    BasisRow {
        synthetic_id: r.synthetic_id.to_string(),
        time: NaiveDateTime::parse_from_str(&r.time, "%F %T")
            .expect("Invalid time format")
            .and_utc(),
        asset: r.asset.to_string(),
        amount: match r.amount.as_str() {
            "" => None,
            _ => Some(KrakenAmount::new(&r.asset, &r.amount).unwrap()),
        },
        exchange_rate: r.exchange_rate.parse::<FiatAmount>().unwrap(),
    }
}

impl<A: Asset> PoolAsset<A> {
    pub fn from_basis_row(basis_row: &BasisRow) -> Self
    where
        <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
    {
        let bucket = Bucket {
            synthetic_id: basis_row.synthetic_id.to_string(),
            time: basis_row.time,
            amount: basis_row.amount.unwrap(),
            exchange_rate: UsdAmount::from(basis_row.exchange_rate),
        };

        Self {
            amount: basis_row.amount.unwrap().try_into().unwrap(),
            lifecycle: BasisLifecycle::lifecycle_from_bucket(bucket),
        }
    }

    pub(crate) fn from_base_deposit(lrd: &LedgerRowDeposit) -> Self
    where
        <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
    {
        Self {
            amount: lrd.amount.try_into().unwrap(),
            lifecycle: BasisLifecycle::lifecycle_from_base(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum LedgerRow {
    DepositRequest(LedgerRowDepositRequest),
    DepositFulfilled(LedgerRowDeposit),
    TransferFutures(LedgerRowDeposit),
    WithdrawalRequest(LedgerRowWithdrawalRequest),
    WithdrawalFulfilled(LedgerRowTypical),
    Trade(LedgerRowTypical),
    Margin(LedgerRowTypical),
    Rollover(LedgerRowTypical),
    SettlePosition(LedgerRowTypical),
}

impl LedgerRow {
    pub(crate) fn to_kraken_csv(&self) -> LedgerCSVRow {
        match self {
            Self::DepositRequest(lrdr) => LedgerCSVRow {
                txid: String::new(),
                refid: lrdr.refid.clone(),
                time: lrdr.time.format("%F %X").to_string(),
                lr_type: self.to_type(),
                subtype: self.to_subtype(),
                aclass: "currency".to_string(),
                asset: lrdr.amount.get_asset().as_kraken().to_string(),
                amount: lrdr.amount.to_kraken_csv(),
                fee: lrdr.fee.to_kraken_csv(),
                balance: String::new(),
            },
            Self::DepositFulfilled(lrd) | Self::TransferFutures(lrd) => LedgerCSVRow {
                txid: lrd.txid.clone(),
                refid: lrd.refid.clone(),
                time: lrd.time.format("%F %X").to_string(),
                lr_type: self.to_type(),
                subtype: self.to_subtype(),
                aclass: "currency".to_string(),
                asset: lrd.amount.get_asset().as_kraken().to_string(),
                amount: lrd.amount.to_kraken_csv(),
                fee: lrd.fee.to_kraken_csv(),
                balance: lrd.balance.to_kraken_csv(),
            },
            Self::WithdrawalRequest(lrwr) => LedgerCSVRow {
                txid: String::new(),
                refid: lrwr.refid.clone(),
                time: lrwr.time.format("%F %X").to_string(),
                lr_type: self.to_type(),
                subtype: self.to_subtype(),
                aclass: "currency".to_string(),
                asset: lrwr.amount.get_asset().as_kraken().to_string(),
                amount: lrwr.amount.to_kraken_csv(),
                fee: lrwr.fee.to_kraken_csv(),
                balance: String::new(),
            },
            Self::WithdrawalFulfilled(lrt)
            | Self::Trade(lrt)
            | Self::Margin(lrt)
            | Self::Rollover(lrt)
            | Self::SettlePosition(lrt) => LedgerCSVRow {
                txid: lrt.txid.clone(),
                refid: lrt.refid.clone(),
                time: lrt.time.format("%F %X").to_string(),
                lr_type: self.to_type(),
                subtype: self.to_subtype(),
                aclass: "currency".to_string(),
                asset: lrt.amount.get_asset().as_kraken().to_string(),
                amount: lrt.amount.to_kraken_csv(),
                fee: lrt.fee.to_kraken_csv(),
                balance: lrt.balance.to_kraken_csv(),
            },
        }
    }

    fn to_type(&self) -> String {
        match self {
            Self::DepositRequest(_) | Self::DepositFulfilled(_) => "deposit",
            Self::TransferFutures(_) => "transfer",
            Self::WithdrawalRequest(_) | Self::WithdrawalFulfilled(_) => "withdrawal",
            Self::Trade(_) => "trade",
            Self::Margin(_) => "margin",
            Self::Rollover(_) => "rollover",
            Self::SettlePosition(_) => "settled",
        }
        .to_string()
    }

    fn to_subtype(&self) -> String {
        match self {
            Self::TransferFutures(_) => "spotfromfutures",
            _ => "",
        }
        .to_string()
    }
}

#[cfg_attr(test, derive(Deserialize, Eq, PartialEq))]
#[derive(Clone, Debug)]
pub struct LedgerRowDeposit {
    pub txid: String,
    pub refid: String,
    pub time: DateTime<Utc>,
    pub amount: KrakenAmount,
    pub fee: KrakenAmount,
    pub balance: KrakenAmount,
}

#[cfg_attr(test, derive(Deserialize, Eq, PartialEq))]
#[derive(Clone, Debug)]
pub struct LedgerRowDepositRequest {
    pub refid: String,
    pub time: DateTime<Utc>,
    pub amount: KrakenAmount,
    pub fee: KrakenAmount,
}

pub(crate) fn parse_lrd(r: &LedgerCSVRow) -> LedgerRowDeposit {
    LedgerRowDeposit {
        txid: r.txid.to_string(),
        refid: r.refid.to_string(),
        time: NaiveDateTime::parse_from_str(&r.time, "%F %T")
            .expect("Invalid time format")
            .and_utc(),
        amount: KrakenAmount::new(&r.asset, &r.amount).unwrap(),
        fee: KrakenAmount::new(&r.asset, &r.fee).unwrap(),
        balance: KrakenAmount::new(&r.asset, &r.balance).unwrap(),
    }
}

pub(crate) fn parse_lrdr(r: &LedgerCSVRow) -> LedgerRowDepositRequest {
    LedgerRowDepositRequest {
        refid: r.refid.to_string(),
        time: NaiveDateTime::parse_from_str(&r.time, "%F %T")
            .expect("Invalid time format")
            .and_utc(),
        amount: KrakenAmount::new(&r.asset, &r.amount).unwrap(),
        fee: KrakenAmount::new(&r.asset, &r.fee).unwrap(),
    }
}

#[derive(Clone, Debug)]
pub struct LedgerRowWithdrawalRequest {
    pub refid: String,
    pub time: DateTime<Utc>,
    pub amount: KrakenAmount,
    pub fee: KrakenAmount,
}

pub(crate) fn parse_lrwr(r: &LedgerCSVRow) -> LedgerRowWithdrawalRequest {
    LedgerRowWithdrawalRequest {
        refid: r.refid.to_string(),
        time: NaiveDateTime::parse_from_str(&r.time, "%F %T")
            .expect("Invalid time format")
            .and_utc(),
        amount: KrakenAmount::new(&r.asset, &r.amount).unwrap(),
        fee: KrakenAmount::new(&r.asset, &r.fee).unwrap(),
    }
}

/// LedgerRowTypical is a helper type for (and owned by) the parent enum type, which raises lr_type to the type system.
#[cfg_attr(test, derive(Eq, PartialEq))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LedgerRowTypical {
    pub txid: String,
    pub refid: String,
    pub time: DateTime<Utc>,  // suffices for display and ordering
    pub amount: KrakenAmount, // ZUSD, XXBT, ZEUR, USDT
    pub fee: KrakenAmount,
    pub balance: KrakenAmount,
}

pub(crate) fn parse_lrt(r: &LedgerCSVRow) -> LedgerRowTypical {
    LedgerRowTypical {
        txid: r.txid.to_string(),
        refid: r.refid.to_string(),
        time: NaiveDateTime::parse_from_str(&r.time, "%F %T")
            .expect("Invalid time format")
            .and_utc(),
        amount: KrakenAmount::new(&r.asset, &r.amount).unwrap(),
        fee: KrakenAmount::new(&r.asset, &r.fee).unwrap(),
        balance: KrakenAmount::new(&r.asset, &r.balance).unwrap(),
    }
}

pub(crate) fn ledger_parse(r: LedgerCSVRow) -> LedgerRow {
    match r.lr_type.as_str() {
        "deposit" => {
            if r.txid.is_empty() {
                LedgerRow::DepositRequest(parse_lrdr(&r))
            } else {
                LedgerRow::DepositFulfilled(parse_lrd(&r))
            }
        }
        "withdrawal" => {
            if r.txid.is_empty() {
                LedgerRow::WithdrawalRequest(parse_lrwr(&r))
            } else {
                LedgerRow::WithdrawalFulfilled(parse_lrt(&r))
            }
        }
        "trade" => LedgerRow::Trade(parse_lrt(&r)),
        "transfer" => {
            if r.subtype == "spotfromfutures" {
                LedgerRow::TransferFutures(parse_lrd(&r))
            } else {
                panic!("ledger_parse() unknown transfer subtype: {}", r.subtype);
            }
        }
        "margin" => LedgerRow::Margin(parse_lrt(&r)),
        "rollover" => LedgerRow::Rollover(parse_lrt(&r)),
        "settled" => LedgerRow::SettlePosition(parse_lrt(&r)),
        _ => panic!("ledger_parse() failed on row {r:?}"),
    }
}

#[derive(Clone, Debug)]
pub enum TradeType {
    Buy,
    Sell,
}

#[derive(Clone, Debug)]
pub enum BookSide {
    Market,
    Limit,
}

#[derive(Clone, Debug)]
pub struct TradeRow {
    pub txid: String,
    pub ordertxid: String,
    pub pair: String, // Trade pair identifier
    pub time: DateTime<Utc>,
    pub tr_type: TradeType,
    pub ordertype: BookSide,
    pub price: KrakenAmount,  // Denominated in the quote asset
    pub cost: KrakenAmount,   // Denominated in the quote asset
    pub fee: KrakenAmount,    // Denominated in the quote asset
    pub vol: KrakenAmount,    // Denominated in the base asset
    pub margin: KrakenAmount, // Denominated in the quote asset
    pub misc: Vec<String>,    // null, closing, initiated
    pub ledgers: Vec<String>, // can be empty.  closing & sells can be 1-2 uuid.  buys can be many.
}

/// Enable consistency checks on years.
impl GetYear for TradeRow {
    fn get_year(&self) -> i32 {
        self.time.year()
    }
}

pub(crate) fn trade_parse(r: TradeCSVRow) -> Result<TradeRow, chrono::ParseError> {
    let (price, cost, fee, vol, margin) = match r.pair.as_str() {
        "USDCCHF" | "USDC/CHF" => (
            KrakenAmount::Chf(r.price.parse().unwrap()),
            KrakenAmount::Chf(r.cost.parse().unwrap()),
            KrakenAmount::Chf(r.fee.parse().unwrap()),
            KrakenAmount::Usdc(r.vol.parse().unwrap()),
            KrakenAmount::Chf(r.margin.parse().unwrap()),
        ),
        "USDCEUR" | "USDC/EUR" => (
            KrakenAmount::Eur(r.price.parse().unwrap()),
            KrakenAmount::Eur(r.cost.parse().unwrap()),
            KrakenAmount::Eur(r.fee.parse().unwrap()),
            KrakenAmount::Usdc(r.vol.parse().unwrap()),
            KrakenAmount::Eur(r.margin.parse().unwrap()),
        ),
        "USDTCHF" | "USDT/CHF" => (
            KrakenAmount::Chf(r.price.parse().unwrap()),
            KrakenAmount::Chf(r.cost.parse().unwrap()),
            KrakenAmount::Chf(r.fee.parse().unwrap()),
            KrakenAmount::Usdt(r.vol.parse().unwrap()),
            KrakenAmount::Chf(r.margin.parse().unwrap()),
        ),
        "USDTEUR" | "USDT/EUR" => (
            KrakenAmount::Eur(r.price.parse().unwrap()),
            KrakenAmount::Eur(r.cost.parse().unwrap()),
            KrakenAmount::Eur(r.fee.parse().unwrap()),
            KrakenAmount::Usdt(r.vol.parse().unwrap()),
            KrakenAmount::Eur(r.margin.parse().unwrap()),
        ),
        "USDTZUSD" | "USDT/USD" => (
            KrakenAmount::Usd(r.price.parse().unwrap()),
            KrakenAmount::Usd(r.cost.parse().unwrap()),
            KrakenAmount::Usd(r.fee.parse().unwrap()),
            KrakenAmount::Usdt(r.vol.parse().unwrap()),
            KrakenAmount::Usd(r.margin.parse().unwrap()),
        ),
        "XETHXXBT" | "ETH/BTC" => (
            KrakenAmount::Btc(r.price.parse().unwrap()),
            KrakenAmount::Btc(r.cost.parse().unwrap()),
            KrakenAmount::Btc(r.fee.parse().unwrap()),
            KrakenAmount::Eth(r.vol.parse().unwrap()),
            KrakenAmount::Btc(r.margin.parse().unwrap()),
        ),
        "XETHZUSD" | "ETH/USD" => (
            KrakenAmount::Usd(r.price.parse().unwrap()),
            KrakenAmount::Usd(r.cost.parse().unwrap()),
            KrakenAmount::Usd(r.fee.parse().unwrap()),
            KrakenAmount::Eth(r.vol.parse().unwrap()),
            KrakenAmount::Usd(r.margin.parse().unwrap()),
        ),
        "XBTCHF" | "BTC/CHF" => (
            KrakenAmount::Chf(r.price.parse().unwrap()),
            KrakenAmount::Chf(r.cost.parse().unwrap()),
            KrakenAmount::Chf(r.fee.parse().unwrap()),
            KrakenAmount::Btc(r.vol.parse().unwrap()),
            KrakenAmount::Chf(r.margin.parse().unwrap()),
        ),
        "XBTUSDC" | "BTC/USDC" => (
            KrakenAmount::Usdc(r.price.parse().unwrap()),
            KrakenAmount::Usdc(r.cost.parse().unwrap()),
            KrakenAmount::Usdc(r.fee.parse().unwrap()),
            KrakenAmount::Btc(r.vol.parse().unwrap()),
            KrakenAmount::Usdc(r.margin.parse().unwrap()),
        ),
        "XBTUSDT" | "BTC/USDT" => (
            KrakenAmount::Usdt(r.price.parse().unwrap()),
            KrakenAmount::Usdt(r.cost.parse().unwrap()),
            KrakenAmount::Usdt(r.fee.parse().unwrap()),
            KrakenAmount::Btc(r.vol.parse().unwrap()),
            KrakenAmount::Usdt(r.margin.parse().unwrap()),
        ),
        "XXBTZEUR" | "BTC/EUR" => (
            KrakenAmount::Eur(r.price.parse().unwrap()),
            KrakenAmount::Eur(r.cost.parse().unwrap()),
            KrakenAmount::Eur(r.fee.parse().unwrap()),
            KrakenAmount::Btc(r.vol.parse().unwrap()),
            KrakenAmount::Eur(r.margin.parse().unwrap()),
        ),
        "XXBTZJPY" | "BTC/JPY" => (
            KrakenAmount::Jpy(r.price.parse().unwrap()),
            KrakenAmount::Jpy(r.cost.parse().unwrap()),
            KrakenAmount::Jpy(r.fee.parse().unwrap()),
            KrakenAmount::Btc(r.vol.parse().unwrap()),
            KrakenAmount::Jpy(r.margin.parse().unwrap()),
        ),
        "XXBTZUSD" | "BTC/USD" => (
            KrakenAmount::Usd(r.price.parse().unwrap()),
            KrakenAmount::Usd(r.cost.parse().unwrap()),
            KrakenAmount::Usd(r.fee.parse().unwrap()),
            KrakenAmount::Btc(r.vol.parse().unwrap()),
            KrakenAmount::Usd(r.margin.parse().unwrap()),
        ),
        _ => panic!("trade_parse() failed on row {r:?}"),
    };

    Ok(TradeRow {
        txid: r.txid.to_string(),
        ordertxid: r.ordertxid.to_string(),
        pair: r.pair.to_string(),
        time: NaiveDateTime::parse_from_str(&r.time, "%F %T%.f")?.and_utc(),
        tr_type: match r.tr_type.as_str() {
            "buy" => TradeType::Buy,
            "sell" => TradeType::Sell,
            _ => panic!("trade_parse_helper() failed in tr_type on row {r:?}"),
        },
        ordertype: match r.ordertype.as_str() {
            "market" => BookSide::Market,
            "limit" => BookSide::Limit,
            _ => panic!("trade_parse_helper() failed in ordertype on row {r:?}"),
        },
        price,
        cost,
        fee,
        vol,
        margin,
        misc: r.misc.split(',').map(|s| s.to_string()).collect(),
        ledgers: r.ledgers.split(',').map(|s| s.to_string()).collect(),
    })
}
