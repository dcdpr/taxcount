use super::*;
use crate::model::kraken_amount::{
    BitcoinAmount, EthWAmount, EtherAmount, FiatAmount, UsdcAmount, UsdtAmount,
    KRAKEN_CRYPTO_INPUT_DIGITS, KRAKEN_FIAT_DIGITS, KRAKEN_STABLECOIN_DIGITS,
};
use crate::model::ledgers::rows::{BookSide, TradeType};
use crate::model::ledgers::rows::{LedgerRowDepositRequest, LedgerRowWithdrawalRequest};
use crate::model::pairs::{get_asset_pair, Pair};
use crate::model::{constants, exchange::Balances, exchange_rate::ExchangeRates};
use arbtest::arbitrary::{Result as ArbResult, Unstructured};
use arbtest::arbtest;
use chrono::{Months, NaiveDateTime, TimeDelta};
use rust_decimal::Decimal;
use similar_asserts::assert_eq;
use std::collections::{BTreeMap, BTreeSet};
use std::{cell::Cell, ops::RangeInclusive, str::FromStr};
use tracing_test::traced_test;

// Most fees are 0.01%, which is equivalent to 0.0001.
const FEE_PERCENT: Decimal = Decimal::from_parts(1, 0, 0, false, 4);

#[test]
#[traced_test]
fn prop_test_parse_ledger_rows() {
    let exchange_rates = ExchangeRates::new(constants::DEFAULT_PATH_EXCHANGE_RATES_DB).unwrap();
    let run_count = Cell::new(0_u64);
    let min_ledger_rows = Cell::new(usize::MAX);
    let max_ledger_rows = Cell::new(0);

    let test = |u: &mut Unstructured<'_>| {
        // Generate a ledger using the stateful Exchange simulation.
        let gen = ExchangeGen::new(u, &exchange_rates)?;

        // Transform the generated ledger rows with the parser.
        let ledgers = FIFO::from_iter(gen.ledger_rows)
            .parse(&FIFO::from(gen.trades_rows))
            .unwrap();

        // Compare parsed `ledgers` against expected rows.
        assert_eq!(FIFO::from_iter(gen.expected_rows.into_iter()), ledgers);

        // Property test statistics.
        run_count.set(run_count.get() + 1);
        let len = ledgers.len();
        if len < min_ledger_rows.get() {
            min_ledger_rows.set(len);
        }
        if len > max_ledger_rows.get() {
            max_ledger_rows.set(len);
        }

        Ok(())
    };

    let entropy_max = 256 * 1024;
    arbtest(&test).size_max(entropy_max).budget_ms(500).run();

    // Require the property test statistics to have certain properties.
    // The test must have run more than 10 times.
    assert!(run_count.get() > 10);
    // At least one run produced 0 ledger rows.
    assert_eq!(min_ledger_rows.get(), 0);
    // At least one run produced more than 1,000 ledger rows.
    assert!(max_ledger_rows.get() > 1_000);

    // Run regression tests.
    let regression_seeds = [0x6dd392720000eecc];
    for seed in regression_seeds {
        arbtest(&test).seed(seed).size_max(entropy_max).run();
    }
}

/// Stateful ledger row generator.
#[derive(Debug)]
struct ExchangeGen {
    /// The exchange maintains its asset balances.
    balances: Balances,

    // Funds held in reserve as collateral for margin trades or pending withdrawals.
    reserved_balances: Balances,

    /// Open margin positions.
    open_positions: BTreeMap<usize, MarginState>,
    position_index: usize,

    /// Deposits are not instant, they are separated into "Request" and "Fulfilled" ledger
    /// events. Amounts in this list are deposit requests. They are removed when fulfilled.
    deposit_requests: Vec<ScheduledFunding>,

    /// Transfer Futures are implemented the same way as deposit requests.
    transfer_futures: Vec<ScheduledFunding>,

    /// Withdrawals are not instant, they are separated into "Request" and "Fulfilled"
    /// ledger events. Amounts in this list are withdrawal requests. They are removed when
    /// fulfilled.
    withdrawal_requests: Vec<ScheduledFunding>,

    /// Record of all ledger rows generated.
    ledger_rows: Vec<LedgerRow>,

    /// Record all expected parsed ledger rows.
    expected_rows: Vec<LedgerParsed>,

    /// Record of trades.csv rows generated.
    trades_rows: HashMap<String, TradeState>,
}

impl ExchangeGen {
    /// Create a new simulated exchange with randomized balances and ledgers.
    fn new(u: &mut Unstructured<'_>, exchange_rates: &ExchangeRates) -> ArbResult<Self> {
        let balances = Balances {
            btc: generate_asset(u, KRAKEN_CRYPTO_INPUT_DIGITS, BIAS_BTC)?,
            chf: generate_fiat_amount(u)?,
            eur: generate_fiat_amount(u)?,
            usd: generate_fiat_amount(u)?,
            eth: generate_asset(u, KRAKEN_CRYPTO_INPUT_DIGITS, BIAS_ETH)?,
            ethw: generate_asset(u, KRAKEN_CRYPTO_INPUT_DIGITS, BIAS_ETH)?,
            jpy: generate_asset(u, KRAKEN_FIAT_DIGITS, BIAS_JPY)?,
            usdc: generate_asset(u, KRAKEN_STABLECOIN_DIGITS, BIAS_STABLECOIN)?,
            usdt: generate_asset(u, KRAKEN_STABLECOIN_DIGITS, BIAS_STABLECOIN)?,
        };
        let mut gen = Self {
            balances,
            reserved_balances: Balances::default(),
            open_positions: BTreeMap::new(),
            position_index: 0,
            deposit_requests: Vec::new(),
            transfer_futures: Vec::new(),
            withdrawal_requests: Vec::new(),
            ledger_rows: Vec::new(),
            expected_rows: Vec::new(),
            trades_rows: HashMap::new(),
        };
        let mut datetime = NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%F %T")
            .unwrap()
            .and_utc();
        let one_year = datetime.checked_add_months(Months::new(12)).unwrap();

        // Generate up to 365 exchange ops. `datetime` is advanced by less than one day,
        // guaranteeing that all ledger rows will fall within 2024.
        loop {
            // Bail early when entropy is exhausted.
            if u.is_empty() {
                break;
            }

            // Choose a ledger operation to perform.
            let op = ExchangeOp::arbitrary(u, &gen)?;

            match op {
                ExchangeOp::Trade => ExchangeOp::trade(u, &mut gen, exchange_rates, datetime)?,
                ExchangeOp::MarginOpen => ExchangeOp::margin_open(u, &mut gen, datetime)?,
                ExchangeOp::MarginOp => {
                    ExchangeOp::margin_op(u, &mut gen, exchange_rates, datetime)?
                }
                ExchangeOp::Deposit => ExchangeOp::deposit_request(u, &mut gen, datetime)?,
                ExchangeOp::TransferFutures => ExchangeOp::transfer_futures(u, &mut gen, datetime)?,
                ExchangeOp::Withdrawal => ExchangeOp::withdrawal_request(u, &mut gen, datetime)?,
            };

            // Advance the datetime by up to 1 day.
            datetime = datetime
                .checked_add_signed(
                    TimeDelta::hours(u.int_in_range(0..=23)?)
                        + TimeDelta::minutes(u.int_in_range(0..=59)?)
                        + TimeDelta::seconds(u.int_in_range(1..=59)?),
                )
                .unwrap();
            if datetime >= one_year {
                break;
            }

            // Handle scheduled deposit and withdrawal requests.
            gen.handle_funding_requests(u, datetime)?;

            // Open margin positions need to rollover every 4 hours, and potential margin calls.
            ExchangeOp::handle_margin_events(u, &mut gen, exchange_rates, datetime)?;

            // Check all balances.
            let any_negative = gen.get_available_balance(AssetName::Btc).is_negative()
                || gen.get_available_balance(AssetName::Chf).is_negative()
                || gen.get_available_balance(AssetName::Eth).is_negative()
                || gen.get_available_balance(AssetName::EthW).is_negative()
                || gen.get_available_balance(AssetName::Eur).is_negative()
                || gen.get_available_balance(AssetName::Jpy).is_negative()
                || gen.get_available_balance(AssetName::Usd).is_negative()
                || gen.get_available_balance(AssetName::Usdc).is_negative()
                || gen.get_available_balance(AssetName::Usdt).is_negative();
            assert!(!any_negative, "Negative balance: {gen:#?}");
        }

        Ok(gen)
    }

    // Get the asset balance that is available for trading or collateral.
    fn get_available_balance(&self, asset: AssetName) -> KrakenAmount {
        let view = BalancesView {
            balances: &self.balances,
            reserved_balances: &self.reserved_balances,
        };
        view.get_available_balance(asset)
    }

    fn has_open_positions(&self) -> bool {
        !self.open_positions.is_empty()
    }

    fn can_settle(
        &self,
        gen: &ExchangeGen,
        exchange_rates: &ExchangeRates,
        datetime: DateTime<Utc>,
    ) -> Option<usize> {
        // Open position can be settled if the borrowed asset can be returned and the loss can be
        // covered.
        self.open_positions
            .iter()
            .find_map(|(&margin_index, margin)| {
                let borrowed_amount = margin.borrowed_amount;
                let outgoing_balance = gen.get_available_balance(borrowed_amount.get_asset());

                let gain_amount = margin.calculate_gain(exchange_rates, datetime);
                let position_asset_balance = gen.get_available_balance(margin.position_asset);

                (!(outgoing_balance - borrowed_amount).is_negative() &&
                // Ensure any loss can be covered
                !(position_asset_balance + gain_amount).is_negative())
                .then_some(margin_index)
            })
    }

    fn can_withdrawal(&self) -> bool {
        // Any asset can be withdrawn.
        self.get_available_balance(AssetName::Btc).is_positive()
            || self.get_available_balance(AssetName::Chf).is_positive()
            || self.get_available_balance(AssetName::Eth).is_positive()
            || self.get_available_balance(AssetName::EthW).is_positive()
            || self.get_available_balance(AssetName::Eur).is_positive()
            || self.get_available_balance(AssetName::Jpy).is_positive()
            || self.get_available_balance(AssetName::Usd).is_positive()
            || self.get_available_balance(AssetName::Usdc).is_positive()
            || self.get_available_balance(AssetName::Usdt).is_positive()
    }

    fn has_tradeable_funding(&self) -> bool {
        // All except EthW.
        self.get_available_balance(AssetName::Btc).is_positive()
            || self.get_available_balance(AssetName::Chf).is_positive()
            || self.get_available_balance(AssetName::Eth).is_positive()
            || self.get_available_balance(AssetName::Eur).is_positive()
            || self.get_available_balance(AssetName::Jpy).is_positive()
            || self.get_available_balance(AssetName::Usd).is_positive()
            || self.get_available_balance(AssetName::Usdc).is_positive()
            || self.get_available_balance(AssetName::Usdt).is_positive()
    }

    fn handle_funding_requests(
        &mut self,
        u: &mut Unstructured<'_>,
        datetime: DateTime<Utc>,
    ) -> ArbResult<()> {
        // Handle deposit requests.
        let mut fulfilled = Vec::new();
        for (i, deposit) in self.deposit_requests.iter().enumerate() {
            if deposit.fulfillment <= datetime {
                let mut view = BalancesMutView {
                    balances: &mut self.balances,
                    reserved_balances: &self.reserved_balances,
                };
                let lrd = view.deposit_inner(u, deposit)?;

                // Insert deposit fulfilled ledger row.
                self.ledger_rows
                    .push(LedgerRow::DepositFulfilled(lrd.clone()));

                // Insert expected row.
                self.expected_rows.push(LedgerParsed::Deposit(lrd));

                fulfilled.push(i);
            }
        }

        // Remove fulfilled deposits.
        for index in fulfilled.into_iter().rev() {
            self.deposit_requests.remove(index);
        }

        // Handle transfer futures.
        let mut fulfilled = Vec::new();
        for (i, future) in self.transfer_futures.iter().enumerate() {
            if future.fulfillment <= datetime {
                let mut view = BalancesMutView {
                    balances: &mut self.balances,
                    reserved_balances: &self.reserved_balances,
                };
                let lrd = view.deposit_inner(u, future)?;

                // Insert transfer futures ledger row.
                self.ledger_rows
                    .push(LedgerRow::TransferFutures(lrd.clone()));

                // Insert expected row.
                // NOTE: `Deposit` is correct for TransferFutures.
                self.expected_rows.push(LedgerParsed::Deposit(lrd));

                fulfilled.push(i);
            }
        }

        // Remove fulfilled deposits.
        for index in fulfilled.into_iter().rev() {
            self.transfer_futures.remove(index);
        }

        // Handle withdrawal requests.
        let mut fulfilled = Vec::new();
        for (i, withdrawal) in self.withdrawal_requests.iter().enumerate() {
            if withdrawal.fulfillment <= datetime {
                let asset = withdrawal.funds.get_asset();
                let fee = fixed_fee(withdrawal.funds, FEE_PERCENT);

                // Decrease both balance and reserved balance by deposit amount, less fees.
                self.balances.accumulate(-withdrawal.funds, fee);
                self.reserved_balances.accumulate(-withdrawal.funds, fee);

                let balance = self.get_available_balance(asset);
                debug_assert!(!balance.is_negative());

                // Insert deposit fulfilled ledger row.
                let lrt = LedgerRowTypical {
                    txid: generate_trade_id(u)?,
                    refid: withdrawal.refid.clone(),
                    time: withdrawal.fulfillment,
                    amount: withdrawal.funds,
                    fee,
                    balance,
                };
                self.ledger_rows
                    .push(LedgerRow::WithdrawalFulfilled(lrt.clone()));

                // Insert expected row.
                self.expected_rows.push(LedgerParsed::Withdrawal(lrt));

                fulfilled.push(i);
            }
        }

        // Remove fulfilled withdrawals.
        for index in fulfilled.into_iter().rev() {
            self.withdrawal_requests.remove(index);
        }

        Ok(())
    }

    // Choose some percentage of funds from any positive balance.
    fn choose_funding(
        &mut self,
        u: &mut Unstructured<'_>,
        percent: Decimal,
    ) -> ArbResult<KrakenAmount> {
        use AssetName::*;

        // Get a list of assets with positive amounts.
        let mut assets = Vec::new();
        for asset in [Btc, Chf, Eth, Eur, Jpy, Usd, Usdc, Usdt] {
            if self.get_available_balance(asset).is_positive() {
                assets.push(asset);
            }
        }

        let asset = u.choose_iter(assets)?;
        let balance = self.get_available_balance(asset).to_decimal();
        let amount = balance * percent;
        let amount = KrakenAmount::try_from_decimal(asset.as_kraken(), amount).unwrap();

        Ok(amount)
    }

    // Reserve funds from the balance.
    fn reserve_funds(&mut self, amount: KrakenAmount) {
        let zero = KrakenAmount::zero(amount.get_asset().as_kraken()).unwrap();
        self.reserved_balances.accumulate(amount, zero);
        debug_assert!(!self.get_available_balance(amount.get_asset()).is_negative());
    }

    // Return reserved funds back to the balance.
    fn return_funds(&mut self, amount: KrakenAmount) {
        let zero = KrakenAmount::zero(amount.get_asset().as_kraken()).unwrap();
        self.reserved_balances.accumulate(-amount, zero);
        debug_assert!(!self.reserved_balances.get(amount.get_asset()).is_negative());
    }
}

#[derive(Debug)]
struct ScheduledFunding {
    refid: String,
    funds: KrakenAmount,
    fulfillment: DateTime<Utc>,
}

impl ScheduledFunding {
    fn generate(
        u: &mut Unstructured<'_>,
        refid: String,
        funds: KrakenAmount,
        requested: DateTime<Utc>,
    ) -> ArbResult<Self> {
        // Generate a fulfillment time that is between 1 minute and 3 days from the request time.
        let fulfillment = requested
            .checked_add_signed(
                TimeDelta::days(u.int_in_range(0..=2)?)
                    + TimeDelta::hours(u.int_in_range(0..=23)?)
                    + TimeDelta::minutes(u.int_in_range(1..=59)?),
            )
            .unwrap();

        Ok(Self {
            refid,
            funds,
            fulfillment,
        })
    }
}

/// Immutable `ExchangeGen` view struct proves disjointness with mutable iterators over its other
/// fields.
///
/// E.g. This allows calling `self.get_available_balances()` while iterating over
/// `self.open_positions.iter_mut()`.
struct BalancesView<'a> {
    balances: &'a Balances,
    reserved_balances: &'a Balances,
}

impl BalancesView<'_> {
    fn get_available_balance(&self, asset: AssetName) -> KrakenAmount {
        self.balances.get(asset) - self.reserved_balances.get(asset)
    }
}

/// Mutable `ExchangeGen` view struct proves disjointness with iterators over its other fields.
///
/// E.g. This allows calling `self.deposit_inner` while iterating over `self.deposit_requests`.
struct BalancesMutView<'a> {
    balances: &'a mut Balances,
    reserved_balances: &'a Balances,
}

impl BalancesMutView<'_> {
    fn deposit_inner(
        &mut self,
        u: &mut Unstructured<'_>,
        deposit: &ScheduledFunding,
    ) -> ArbResult<LedgerRowDeposit> {
        let asset = deposit.funds.get_asset();
        let zero = KrakenAmount::zero(asset.as_kraken()).unwrap();

        // Increase balance by deposit amount.
        self.balances.accumulate(deposit.funds, zero);

        let balance = self.view().get_available_balance(asset);
        debug_assert!(!balance.is_negative());

        Ok(LedgerRowDeposit {
            txid: generate_trade_id(u)?,
            refid: deposit.refid.clone(),
            time: deposit.fulfillment,
            amount: deposit.funds,
            fee: zero,
            balance,
        })
    }

    /// Convert this mutable view into an immutable view.
    fn view(&self) -> BalancesView<'_> {
        BalancesView {
            balances: self.balances,
            reserved_balances: self.reserved_balances,
        }
    }
}

#[derive(Debug)]
struct MarginState {
    trade_id: String,
    borrowed_amount: KrakenAmount,
    fee_for_borrowed_amount: KrakenAmount,
    collateral_amount: KrakenAmount,
    position_asset: AssetName,
    open_time: DateTime<Utc>,
    last_update_time: DateTime<Utc>,
}

impl MarginState {
    fn new(
        trade_id: String,
        borrowed_amount: KrakenAmount,
        fee_for_borrowed_amount: KrakenAmount,
        collateral_amount: KrakenAmount,
        position_asset: AssetName,
        datetime: DateTime<Utc>,
    ) -> Self {
        Self {
            trade_id,
            borrowed_amount,
            fee_for_borrowed_amount,
            collateral_amount,
            position_asset,
            open_time: datetime,
            last_update_time: datetime,
        }
    }
}

impl MarginState {
    fn calculate_gain(
        &self,
        exchange_rates: &ExchangeRates,
        datetime: DateTime<Utc>,
    ) -> KrakenAmount {
        // Profit or loss is the difference between the quote amount at open time
        // and close time.
        let position_amount_at_open = KrakenAmount::convert(
            self.borrowed_amount, // Quote asset, e.g. USD
            self.position_asset,  // Base asset, e.g. BTC
            exchange_rates,
            self.open_time,
        )
        .unwrap();
        let position_amount_at_close = KrakenAmount::convert(
            self.borrowed_amount, // Quote asset, e.g. USD
            self.position_asset,  // Base asset, e.g. BTC
            exchange_rates,
            datetime,
        )
        .unwrap();

        position_amount_at_close - position_amount_at_open
    }
}

#[derive(Debug)]
struct TradeState {
    pair: &'static str,
    datetime: DateTime<Utc>,
    price: KrakenAmount,
    misc: &'static str,
}

impl TradeState {
    fn new(
        pair: &'static str,
        datetime: DateTime<Utc>,
        price: KrakenAmount,
        misc: &'static str,
    ) -> Self {
        Self {
            pair,
            datetime,
            price,
            misc,
        }
    }
}

impl From<(String, TradeState)> for TradeRow {
    fn from((txid, trade_state): (String, TradeState)) -> Self {
        let zero = KrakenAmount::zero(trade_state.price.get_asset().as_kraken()).unwrap();
        Self {
            txid,
            ordertxid: "TODO-OrderId".to_string(),
            pair: trade_state.pair.to_string(),
            time: trade_state.datetime,
            tr_type: TradeType::Buy, // TODO: `TradeState` needs the trade pair.
            ordertype: BookSide::Limit, // TODO: `TradeState` needs the order type.
            price: trade_state.price,
            cost: zero,   // TODO: `TradeState` needs more info.
            fee: zero,    // TODO: `TradeState` needs more info.
            vol: zero,    // TODO: `TradeState` needs more info.
            margin: zero, // TODO: `TradeState` needs more info.
            misc: trade_state
                .misc
                .split(",")
                .map(|item| item.to_string())
                .collect(),
            ledgers: Vec::new(), // TODO: `TradeState` needs to record associated Ledger IDs.
        }
    }
}

impl From<HashMap<String, TradeState>> for FIFO<TradeRow> {
    fn from(value: HashMap<String, TradeState>) -> Self {
        Self::from_iter(value.into_iter().map(|(k, v)| TradeRow::from((k, v))))
    }
}

/// The exchange operations roughly map to `LedgerRow` and `LedgerParsed`.
#[derive(Copy, Clone, Debug)]
enum ExchangeOp {
    Trade,
    MarginOpen,
    MarginOp,
    Deposit,
    TransferFutures,
    Withdrawal,
}

impl ExchangeOp {
    fn arbitrary(u: &mut Unstructured<'_>, gen: &ExchangeGen) -> ArbResult<Self> {
        let has_tradeable_funding = gen.has_tradeable_funding();

        // Create a list of valid exchange operations, based on generator state.
        let ops = [
            has_tradeable_funding.then_some(Self::Trade),
            has_tradeable_funding.then_some(Self::MarginOpen),
            gen.has_open_positions().then_some(Self::MarginOp),
            Some(Self::Deposit),
            Some(Self::TransferFutures),
            gen.can_withdrawal().then_some(Self::Withdrawal),
        ];

        u.choose_iter(ops.into_iter().flatten().collect::<Vec<_>>())
    }

    fn trade(
        u: &mut Unstructured<'_>,
        gen: &mut ExchangeGen,
        exchange_rates: &ExchangeRates,
        datetime: DateTime<Utc>,
    ) -> ArbResult<()> {
        let pair = generate_trade_pair(u, gen)?;
        let asset_out = pair.get_base();
        let asset_in = pair.get_quote();

        let from_balance = gen.get_available_balance(asset_out);

        // Sell up to half of the balance.
        let amount = from_balance.to_decimal() * decimal_percent(u, 0..=50)?;
        let mut amount_out = KrakenAmount::try_from_decimal(asset_out.as_kraken(), amount).unwrap();

        // Receive the amount converted from the outgoing asset.
        let mut amount_in = amount_out
            .convert(asset_in, exchange_rates, datetime)
            .unwrap();

        // Randomize fees: Choose between charging the fee to either the outgoing or
        // incoming asset, then randomize the amount between 0-1% of the trade
        // amount.
        let zero_out = KrakenAmount::zero(asset_out.as_kraken()).unwrap();
        let zero_in = KrakenAmount::zero(asset_in.as_kraken()).unwrap();
        let (fee_out, fee_in) = if u.ratio(1, 2)? {
            let fee = generate_fee(u, amount_out)?;

            // Subtract the fee from the outgoing amount.
            amount_out = amount_out - fee;

            (fee, zero_in)
        } else {
            let fee = generate_fee(u, amount_in)?;

            // Subtract the fee from the incoming amount.
            amount_in = amount_in - fee;

            (zero_out, fee)
        };

        // Update balances.
        gen.balances.accumulate(-amount_out, zero_out);
        gen.balances.accumulate(amount_in, zero_in);

        // Generate outgoing ledger row.
        let trade_id = generate_trade_id(u)?;
        let row_out = LedgerRowTypical {
            txid: generate_ledger_id(u)?,
            refid: trade_id.clone(),
            time: datetime,
            amount: -amount_out,
            fee: fee_out,
            balance: from_balance - amount_out,
        };
        gen.ledger_rows.push(LedgerRow::Trade(row_out.clone()));

        // Generate incoming ledger row.
        let row_in = LedgerRowTypical {
            txid: generate_ledger_id(u)?,
            refid: trade_id,
            time: datetime,
            amount: amount_in,
            fee: fee_in,
            balance: gen.balances.get(asset_in) + amount_in,
        };
        gen.ledger_rows.push(LedgerRow::Trade(row_in.clone()));

        // Insert expected row.
        gen.expected_rows
            .push(LedgerParsed::Trade { row_out, row_in });

        Ok(())
    }

    fn margin_op(
        u: &mut Unstructured<'_>,
        gen: &mut ExchangeGen,
        exchange_rates: &ExchangeRates,
        datetime: DateTime<Utc>,
    ) -> ArbResult<()> {
        /// Margin position sub-operation
        #[derive(Copy, Clone, Debug)]
        enum SubOp {
            Close,
            Settle(usize),
        }

        impl SubOp {
            fn arbitrary(
                u: &mut Unstructured<'_>,
                gen: &ExchangeGen,
                exchange_rates: &ExchangeRates,
                datetime: DateTime<Utc>,
            ) -> ArbResult<Self> {
                // Choose between close or settle, if settlement is possible.
                let mut ops = vec![Self::Close];

                if let Some(margin_index) = gen.can_settle(gen, exchange_rates, datetime) {
                    ops.push(Self::Settle(margin_index));
                }

                u.choose(&ops).copied()
            }
        }

        match SubOp::arbitrary(u, gen, exchange_rates, datetime)? {
            SubOp::Close => Self::margin_close(u, gen, exchange_rates)?,
            SubOp::Settle(margin_index) => {
                Self::margin_settle(u, gen, margin_index, exchange_rates, datetime)?
            }
        }

        Ok(())
    }

    // See: https://support.kraken.com/hc/en-us/articles/203053116-How-leverage-works-in-spot-transactions-on-margin
    fn margin_open(
        u: &mut Unstructured<'_>,
        gen: &mut ExchangeGen,
        datetime: DateTime<Utc>,
    ) -> ArbResult<()> {
        let result = generate_margin_pair(u, gen);
        let pair = match result {
            Ok(pair) => pair,
            Err(arbtest::arbitrary::Error::EmptyChoose) => return Ok(()),
            Err(err) => return Err(err),
        };
        let borrowed_asset = pair.get_base();
        let position_asset = pair.get_quote();

        // Use 3-50% of available balance as collateral.
        let percent = decimal_percent(u, 3..=50)?;
        let collateral_amount = gen.choose_funding(u, percent)?;

        gen.reserve_funds(collateral_amount);

        let leverage = u.choose_iter(1_i8..=5)?;
        let leverage_amount = collateral_amount.to_decimal() * Decimal::from(leverage);
        let mut borrowed_amount =
            KrakenAmount::try_from_decimal(borrowed_asset.as_kraken(), leverage_amount).unwrap();

        // In the case that the borrowed asset and collateral asset are identical, e.g. leveraging
        // BTC with BTC, the closing fee will always be 0.01% of up to half of the BTC balance.
        // Which will never trigger the negative assertion.
        if borrowed_asset != collateral_amount.get_asset() {
            // Multiply the balance by 200, which is proportional to the 0.01% opening fee AND
            // 0.1% closing fee.
            let balance_for_borrowed_asset =
                gen.get_available_balance(borrowed_asset).to_decimal() * Decimal::from(200);
            let scaled_balance = KrakenAmount::try_from_decimal(
                borrowed_asset.as_kraken(),
                balance_for_borrowed_asset,
            )
            .unwrap();
            borrowed_amount = borrowed_amount.min(scaled_balance);
        }

        // Decide to take fees out of either the borrowed asset or collateral asset.
        let fee_from_borrowed =
            gen.get_available_balance(borrowed_asset).is_positive() && u.ratio(1, 2)?;

        let (fee, zero) = if fee_from_borrowed {
            // Take the 0.01% opening fee out of the borrowed amount.
            let fee = fixed_fee(borrowed_amount, FEE_PERCENT);
            let zero = KrakenAmount::zero(borrowed_asset.as_kraken()).unwrap();

            (fee, zero)
        } else {
            // Take the 0.01% opening fee out of the collateral amount.
            let fee = fixed_fee(collateral_amount, FEE_PERCENT);
            let zero = KrakenAmount::zero(collateral_amount.get_asset().as_kraken()).unwrap();

            (fee, zero)
        };
        gen.balances.accumulate(zero, fee);

        // Reserve the closing fee.
        gen.reserve_funds(fee);

        // Open margin position.
        let trade_id = generate_trade_id(u)?;
        gen.open_positions.insert(
            gen.position_index,
            MarginState::new(
                trade_id.clone(),
                borrowed_amount,
                fee,
                collateral_amount,
                position_asset,
                datetime,
            ),
        );
        gen.position_index += 1;

        // Generate ledger row.
        let row = LedgerRowTypical {
            txid: generate_ledger_id(u)?,
            refid: trade_id,
            time: datetime,
            amount: zero,
            fee,
            balance: gen.balances.get(zero.get_asset()),
        };
        gen.ledger_rows.push(LedgerRow::Margin(row.clone()));

        // Insert expected row.
        gen.expected_rows
            .push(LedgerParsed::MarginPositionOpen(row));

        Ok(())
    }

    fn margin_close(
        u: &mut Unstructured<'_>,
        gen: &mut ExchangeGen,
        exchange_rates: &ExchangeRates,
    ) -> ArbResult<()> {
        let index = *u.choose_iter(gen.open_positions.keys())?;
        let margin = gen.open_positions.get(&index).unwrap();
        gen.return_funds(margin.collateral_amount);

        let margin = gen.open_positions.get(&index).unwrap();

        let fee = margin.fee_for_borrowed_amount;
        let balance = gen.get_available_balance(fee.get_asset());
        let zero = KrakenAmount::zero(fee.get_asset().as_kraken()).unwrap();

        // Pay the closing fee out of the reserved balances.
        gen.reserved_balances.accumulate(zero, fee);

        // Insert one or two ledger rows.
        Self::insert_margin_close_rows(
            u,
            &mut gen.ledger_rows,
            &mut gen.trades_rows,
            &mut gen.expected_rows,
            margin,
            exchange_rates,
            balance,
        )?;

        // Remove the open position.
        gen.open_positions.remove(&index);

        Ok(())
    }

    // See: https://support.kraken.com/hc/en-us/articles/settling-a-spot-position-on-margin-on-kraken-pro
    fn margin_settle(
        u: &mut Unstructured<'_>,
        gen: &mut ExchangeGen,
        margin_index: usize,
        exchange_rates: &ExchangeRates,
        datetime: DateTime<Utc>,
    ) -> ArbResult<()> {
        let margin = gen.open_positions.get(&margin_index).unwrap();
        let collateral_amount = margin.collateral_amount;
        let fee_for_borrowed_amount = margin.fee_for_borrowed_amount;

        gen.return_funds(collateral_amount);

        // Return the reserved closing fee.
        gen.return_funds(fee_for_borrowed_amount);

        let margin = gen.open_positions.get(&margin_index).unwrap();

        // Give back what was borrowed, straight from your balances.
        // See: doc/notes-on-margin.txt
        let borrowed_amount = margin.borrowed_amount;
        let outgoing_asset_zero =
            KrakenAmount::zero(borrowed_amount.get_asset().as_kraken()).unwrap();

        let gain_amount = margin.calculate_gain(exchange_rates, datetime);
        let position_asset_zero = KrakenAmount::zero(margin.position_asset.as_kraken()).unwrap();

        gen.balances
            .accumulate(-borrowed_amount, outgoing_asset_zero);
        gen.balances.accumulate(gain_amount, position_asset_zero);

        // Generate outgoing ledger row.
        let trade_id = generate_trade_id(u)?;
        let row_out = LedgerRowTypical {
            txid: generate_ledger_id(u)?,
            refid: trade_id.clone(),
            time: datetime,
            amount: -borrowed_amount,
            fee: outgoing_asset_zero,
            balance: gen.balances.get(borrowed_amount.get_asset()),
        };
        gen.ledger_rows
            .push(LedgerRow::SettlePosition(row_out.clone()));

        // Generate profit/loss ledger row.
        let row_in = LedgerRowTypical {
            txid: generate_ledger_id(u)?,
            refid: trade_id,
            time: datetime,
            amount: gain_amount,
            fee: position_asset_zero,
            balance: gen.balances.get(margin.position_asset),
        };
        gen.ledger_rows
            .push(LedgerRow::SettlePosition(row_in.clone()));

        // Insert expected row.
        gen.expected_rows
            .push(LedgerParsed::MarginPositionSettle { row_out, row_in });

        // Remove the open position.
        gen.open_positions.remove(&margin_index);

        Ok(())
    }

    fn handle_margin_events(
        u: &mut Unstructured<'_>,
        gen: &mut ExchangeGen,
        exchange_rates: &ExchangeRates,
        datetime: DateTime<Utc>,
    ) -> ArbResult<()> {
        let four_hours = TimeDelta::hours(4);
        let mut updated;

        loop {
            let mut closing = BTreeSet::new();
            updated = false;

            for (i, margin) in gen.open_positions.iter_mut() {
                // Bail early when entropy is exhausted.
                if u.is_empty() {
                    break;
                }

                if closing.contains(i) {
                    continue;
                }

                if margin.last_update_time + four_hours < datetime {
                    updated = true;
                    margin.last_update_time += four_hours;

                    // Take 0.01% of the borrowed amount for the fee.
                    let fee = margin.fee_for_borrowed_amount;
                    let view = BalancesView {
                        balances: &gen.balances,
                        reserved_balances: &gen.reserved_balances,
                    };
                    let asset = fee.get_asset();
                    let balance = view.get_available_balance(asset);
                    let zero = KrakenAmount::zero(asset.as_kraken()).unwrap();

                    // Choosing twice the fee is arbitrarily conservative.
                    if balance.to_decimal() < fee.to_decimal() * Decimal::from(2) {
                        // Apply margin call by closing the position.

                        // Pay the closing fee out of the reserved balances.
                        gen.reserved_balances.accumulate(zero, fee);

                        // Defer removal of open position.
                        closing.insert(*i);

                        // Insert one or two ledger rows.
                        Self::insert_margin_close_rows(
                            u,
                            &mut gen.ledger_rows,
                            &mut gen.trades_rows,
                            &mut gen.expected_rows,
                            margin,
                            exchange_rates,
                            balance,
                        )?;
                    } else {
                        // Pay the rollover fee.
                        gen.balances.accumulate(zero, fee);

                        let row = LedgerRowTypical {
                            txid: generate_ledger_id(u)?,
                            refid: margin.trade_id.clone(),
                            time: margin.last_update_time,
                            amount: zero,
                            fee,
                            balance: gen.balances.get(asset),
                        };

                        // Insert expected row.
                        gen.expected_rows
                            .push(LedgerParsed::MarginPositionRollover(row.clone()));

                        gen.ledger_rows.push(LedgerRow::Rollover(row));
                    };
                }
            }

            // Remove closing positions.
            for index in closing.iter().rev() {
                gen.open_positions.remove(index);
            }
            closing.clear();

            if !updated {
                break;
            }
        }

        Ok(())
    }

    fn insert_margin_close_rows(
        u: &mut Unstructured<'_>,
        ledger_rows: &mut Vec<LedgerRow>,
        trades_rows: &mut HashMap<String, TradeState>,
        expected_rows: &mut Vec<LedgerParsed>,
        margin: &MarginState,
        exchange_rates: &ExchangeRates,
        balance: KrakenAmount,
    ) -> ArbResult<()> {
        let trade_id = generate_trade_id(u)?;

        // Generate closing event in trades.csv.
        let pair = get_asset_pair(margin.position_asset, margin.borrowed_amount.get_asset())
            .0
            .as_kraken();
        let price = KrakenAmount::convert(
            margin.borrowed_amount, // Quote asset, e.g. USD
            margin.position_asset,  // Base asset, e.g. BTC
            exchange_rates,
            margin.open_time,
        )
        .unwrap();

        trades_rows.insert(
            trade_id.clone(),
            TradeState::new(pair, margin.open_time, price, "closing"),
        );

        let borrowed_asset = margin.borrowed_amount.get_asset();
        let borrowed_asset_zero = KrakenAmount::zero(borrowed_asset.as_kraken()).unwrap();

        // Decide if the fee should be paid in the same asset as the proceeds.
        let fee = margin.fee_for_borrowed_amount;
        let collapse_rows = borrowed_asset == fee.get_asset();

        // Generate proceeds row for closing event.
        let row_proceeds = LedgerRowTypical {
            txid: generate_ledger_id(u)?,
            refid: trade_id,
            time: margin.last_update_time,
            // TODO: This always "awards" the trader with the entire borrowed amount,
            // and does not accumulate into balances.
            amount: margin.borrowed_amount,
            fee: if collapse_rows {
                fee
            } else {
                borrowed_asset_zero
            },
            balance, // TODO: balance is wrong here.
        };
        ledger_rows.push(LedgerRow::Margin(row_proceeds.clone()));

        let fee_asset = kraken_asset_from_pair(pair);
        let fee_asset_zero = KrakenAmount::zero(fee_asset).unwrap();

        // Decide to generate the margin close as either one or two rows.
        let row_fee = if collapse_rows {
            // For single-row closing, the expected fee row is basically empty.
            MarginFeeRow {
                txid: row_proceeds.txid.clone(),
                refid: row_proceeds.refid.clone(),
                time: row_proceeds.time,
                amount: fee_asset_zero,
                fee: fee_asset_zero,
                balance: None,
            }
        } else {
            // For two-row closing, the fee is non-zero and is added to the ledger.
            let row_fee = MarginFeeRow {
                txid: generate_ledger_id(u)?,
                refid: row_proceeds.refid.clone(),
                time: row_proceeds.time,
                amount: fee_asset_zero,
                fee,
                balance: Some(balance),
            };

            ledger_rows.push(LedgerRow::Margin((&row_fee).into()));

            row_fee
        };

        // Insert expected row.
        expected_rows.push(LedgerParsed::MarginPositionClose {
            row_proceeds,
            row_fee,
            exchange_rate: price,
        });

        Ok(())
    }

    fn deposit_request(
        u: &mut Unstructured<'_>,
        gen: &mut ExchangeGen,
        datetime: DateTime<Utc>,
    ) -> ArbResult<()> {
        use AssetName::*;

        // Any asset can be deposited in any amount.
        let asset = *u.choose(&[Btc, Chf, Eth, EthW, Eur, Jpy, Usd, Usdc, Usdt])?;
        let amount = match asset {
            Btc => {
                let amount: BitcoinAmount =
                    generate_asset(u, KRAKEN_CRYPTO_INPUT_DIGITS, BIAS_BTC)?;
                KrakenAmount::from(amount)
            }
            Chf | Eur | Usd => KrakenAmount::try_from((asset, generate_fiat_amount(u)?)).unwrap(),
            Eth | EthW => {
                let amount: EtherAmount = generate_asset(u, KRAKEN_CRYPTO_INPUT_DIGITS, BIAS_ETH)?;
                KrakenAmount::from(amount)
            }
            Jpy => {
                let fiat_amount: FiatAmount = generate_asset(u, KRAKEN_FIAT_DIGITS, BIAS_JPY)?;
                KrakenAmount::try_from((asset, fiat_amount)).unwrap()
            }
            Usdc => {
                let amount: UsdcAmount =
                    generate_asset(u, KRAKEN_STABLECOIN_DIGITS, BIAS_STABLECOIN)?;
                KrakenAmount::from(amount)
            }
            Usdt => {
                let amount: UsdtAmount =
                    generate_asset(u, KRAKEN_STABLECOIN_DIGITS, BIAS_STABLECOIN)?;
                KrakenAmount::from(amount)
            }
        };

        let refid = generate_trade_id(u)?;
        let funding = ScheduledFunding::generate(u, refid.clone(), amount, datetime)?;
        gen.deposit_requests.push(funding);

        // Insert deposit request ledger row.
        let lrdr = LedgerRowDepositRequest {
            refid,
            time: datetime,
            amount,
            fee: KrakenAmount::zero(amount.get_asset().as_kraken()).unwrap(),
        };
        gen.ledger_rows.push(LedgerRow::DepositRequest(lrdr));

        Ok(())
    }

    fn transfer_futures(
        u: &mut Unstructured<'_>,
        gen: &mut ExchangeGen,
        datetime: DateTime<Utc>,
    ) -> ArbResult<()> {
        // TODO: Only supporting EthW TransferFutures because that's what has been used in practice.
        let amount: EthWAmount = generate_asset(u, KRAKEN_CRYPTO_INPUT_DIGITS, BIAS_ETH)?;
        let amount = KrakenAmount::from(amount);

        let refid = generate_trade_id(u)?;
        let funding = ScheduledFunding::generate(u, refid.clone(), amount, datetime)?;
        gen.transfer_futures.push(funding);

        // Insert deposit request ledger row.
        // NOTE: `deposit request` is correct for TransferFutures.
        let lrdr = LedgerRowDepositRequest {
            refid,
            time: datetime,
            amount,
            fee: KrakenAmount::zero(amount.get_asset().as_kraken()).unwrap(),
        };
        gen.ledger_rows.push(LedgerRow::DepositRequest(lrdr));

        Ok(())
    }

    fn withdrawal_request(
        u: &mut Unstructured<'_>,
        gen: &mut ExchangeGen,
        datetime: DateTime<Utc>,
    ) -> ArbResult<()> {
        // Generate a funding asset with 1-100% of current balance.
        let percent = decimal_percent(u, 1..=100)?;
        let result = gen.choose_funding(u, percent);
        let amount = match result {
            Ok(amount) => amount,
            Err(arbtest::arbitrary::Error::EmptyChoose) => return Ok(()),
            Err(err) => return Err(err),
        };

        // Reserve funds for withdrawal.
        gen.reserve_funds(amount);

        // Take the 0.01% withdrawal fee out of the reserved amount.
        let fee = fixed_fee(amount, FEE_PERCENT);
        let amount = amount - fee;

        let refid = generate_trade_id(u)?;
        let funding = ScheduledFunding::generate(u, refid.clone(), amount, datetime)?;
        gen.withdrawal_requests.push(funding);

        // Insert withdrawal request ledger row.
        let lrwr = LedgerRowWithdrawalRequest {
            refid,
            time: datetime,
            amount,
            fee,
        };
        gen.ledger_rows.push(LedgerRow::WithdrawalRequest(lrwr));

        Ok(())
    }
}

fn generate_ledger_id(u: &mut Unstructured<'_>) -> ArbResult<String> {
    Ok(format!("L{}", generate_id(u)?))
}

fn generate_trade_id(u: &mut Unstructured<'_>) -> ArbResult<String> {
    Ok(format!("T{}", generate_id(u)?))
}

fn generate_id(u: &mut Unstructured<'_>) -> ArbResult<String> {
    let mut txid = String::new();

    const CHARS: [char; 36] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
        'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    ];

    for _ in 0..5 {
        txid.push(*u.choose(&CHARS)?);
    }
    txid.push('-');
    for _ in 0..5 {
        txid.push(*u.choose(&CHARS)?);
    }
    txid.push('-');
    for _ in 0..6 {
        txid.push(*u.choose(&CHARS)?);
    }

    Ok(txid)
}

// 10% of generated values will be 0 BTC.
// 89.1% of generated values will be less than 10 BTC.
// 0.9% of generated values will be 0-10,000 BTC.
const BIAS_BTC: RangeInclusive<u8> = 1..=4;

// 10% of generated values will be 0 ETH.
// 89.1% of generated values will be less than 100 ETH.
// 0.9% of generated values will be 0-100,000 ETH.
const BIAS_ETH: RangeInclusive<u8> = 2..=5;

// 10% of generated values will be 0.
// 89.1% of generated values will be less than 100,000.
// 0.9% of generated values will be 0-100,000,000.
const BIAS_FIAT: RangeInclusive<u8> = 5..=8;

// 10% of generated values will be ¥0.
// 89.1% of generated values will be less than ¥10,000,000.
// 0.9% of generated values will be ¥0-¥10,000,000,000.
const BIAS_JPY: RangeInclusive<u8> = 7..=10;

// 10% of generated values will be 0 USDC/USDT.
// 89.1% of generated values will be less than 100,000 USDC/USDT.
// 0.9% of generated values will be 0-100,000,000 USDC/USDT.
const BIAS_STABLECOIN: RangeInclusive<u8> = 5..=8;

fn generate_fiat_amount(u: &mut Unstructured<'_>) -> ArbResult<FiatAmount> {
    generate_asset(u, 4, BIAS_FIAT)
}

/// Parametric asset generator.
///
/// Values generated are biased toward:
///
/// | Maximum Value     | Probability |
/// |-------------------|-------------|
/// | `0`               | 10%         |
/// | `10^bias.start`   | 89.1%       |
/// | `10^bias.end`     | 0.9%        |
fn generate_asset<A>(
    u: &mut Unstructured<'_>,
    precision: u8,
    bias: RangeInclusive<u8>,
) -> ArbResult<A>
where
    A: FromStr,
    <A as FromStr>::Err: std::fmt::Debug,
{
    const DIGITS: [char; 10] = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];

    let value = if u.ratio(1, 10)? {
        // Generate zero 10% of the time.
        format!("0.{}", "0".repeat(precision as usize))
    } else {
        // Otherwise generate a totally random value.
        let mut value = String::new();

        // The number of significant digits will be `bias.end` 1% of the time.
        let leading_digits = *if u.ratio(1, 100)? {
            bias.end()
        } else {
            bias.start()
        };

        // Add random significant digits.
        for _ in 1..=u.int_in_range(1..=leading_digits)? {
            value.push(*u.choose(&DIGITS)?);
        }

        // Decimal point
        value.push('.');

        // Add random trailing digits.
        for _ in 0..precision {
            value.push(*u.choose(&DIGITS)?);
        }

        value
    };

    Ok(value.parse().unwrap())
}

/// Randomize fees: The amount will be between 0-1% of the provided amount.
fn generate_fee(u: &mut Unstructured<'_>, amount: KrakenAmount) -> ArbResult<KrakenAmount> {
    let fee_percent = decimal_percent(u, 0..=1)?;

    Ok(fixed_fee(amount, fee_percent))
}

/// Generate a fee with fixed percentage.
fn fixed_fee(amount: KrakenAmount, fee_percent: Decimal) -> KrakenAmount {
    let fee_amount = amount.to_decimal() * fee_percent;

    KrakenAmount::try_from_decimal(amount.get_asset().as_kraken(), fee_amount).unwrap()
}

fn generate_trade_pair(u: &mut Unstructured<'_>, gen: &ExchangeGen) -> ArbResult<Pair> {
    use Pair::*;

    // Create a list of valid trade pairs, based on generator state.
    let mut pairs = Vec::new();

    if gen.get_available_balance(AssetName::Btc).is_positive() {
        pairs.extend([BtcChf, BtcEur, BtcJpy, BtcUsd, BtcUsdc, BtcUsdt]);
    }
    if gen.get_available_balance(AssetName::Eth).is_positive() {
        pairs.extend([EthBtc, EthChf, EthEur, EthJpy, EthUsd, EthUsdc, EthUsdt]);
    }
    if gen.get_available_balance(AssetName::Eur).is_positive() {
        pairs.extend([EurChf, EurJpy, EurUsd]);
    }
    if gen.get_available_balance(AssetName::Usd).is_positive() {
        pairs.extend([UsdChf, UsdJpy]);
    }
    if gen.get_available_balance(AssetName::Usdc).is_positive() {
        pairs.extend([UsdcChf, UsdcEur, UsdcUsd, UsdcUsdt]);
    }
    if gen.get_available_balance(AssetName::Usdt).is_positive() {
        pairs.extend([UsdtChf, UsdtEur, UsdtJpy, UsdtUsd]);
    }

    u.choose_iter(pairs)
}

fn generate_margin_pair(u: &mut Unstructured<'_>, gen: &ExchangeGen) -> ArbResult<Pair> {
    use Pair::*;

    // Create a list of valid trade pairs for margin trades, based on generator state.
    // TODO: This only uses asset pairs that have been used in practice.
    let mut pairs = Vec::new();

    if gen.get_available_balance(AssetName::Btc).is_positive() {
        pairs.extend([BtcChf, BtcEur, BtcJpy, BtcUsd, BtcUsdc, BtcUsdt]);
    }
    if gen.get_available_balance(AssetName::Eth).is_positive() {
        pairs.extend([EthBtc, EthUsd]);
    }
    if gen.get_available_balance(AssetName::Usdc).is_positive() {
        pairs.extend([UsdcChf, UsdcEur]);
    }
    if gen.get_available_balance(AssetName::Usdt).is_positive() {
        pairs.extend([UsdtChf, UsdtEur, UsdtUsd]);
    }

    u.choose_iter(pairs)
}

// Produce a `Decimal` that represents a percentage in the range `0..=100`
fn decimal_percent(u: &mut Unstructured<'_>, range: RangeInclusive<i64>) -> ArbResult<Decimal> {
    let range = RangeInclusive::new(range.start() * 100_000, range.end() * 100_000);
    let denominator = u.int_in_range(range)?;

    Ok(Decimal::new(denominator, 7))
}
