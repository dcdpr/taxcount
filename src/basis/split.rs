use super::lookup::{BasisLookup, BasisLookupExt};
use super::ToNonSplittable as _;
use super::{Asset, AssetName, BasisLifecycle, PoolAsset, PoolAssetSplit};
use crate::errors::ExchangeRateError;
use crate::imports::wallet::{LoanRole, Tx, TxType};
use crate::model::blockchain::{BlockchainExt, TimeOrderedBlockchain};
use crate::model::checkpoint::{Pending, PendingAccountTx, PendingTxInfo, PendingUtxo, State};
use crate::model::events::{Event, GainConfig, WalletDirection};
use crate::model::kraken_amount::{BitcoinAmount, EthWAmount, EtherAmount, UsdcAmount, UsdtAmount};
use crate::model::kraken_amount::{FiatAmount, KrakenAmount, UsdAmount};
use crate::model::ledgers::parsed::{LedgerMarginClose, LedgerParsed, LedgerTwoRowTrade};
use crate::model::ledgers::rows::{BasisRow, LedgerRowDeposit, TradeRow};
use crate::model::pairs::{get_asset_pair, Trade};
use crate::util::fifo::FIFO;
use chrono::{DateTime, Utc};
use error_iter::ErrorIter as _;
use std::collections::{HashMap, HashSet};
use std::ops::{Add, Sub};
use std::{fmt::Write as _, rc::Rc, time::Duration};
use thiserror::Error;
use tracing::{debug, trace};

// TODO: A 2-hour search window might be too aggressive. Deposits and withdrawals may take far
// longer when the Bitcoin network has trouble making progress. E.g. when mining a block takes
// longer than usual.
const HEURISTIC_SEARCH_WINDOW_BLOCKWAIT: Duration = Duration::from_secs(60 * 60 * 2);
const HEURISTIC_SEARCH_WINDOW_MISORDERED: Duration = Duration::from_secs(60 * 10);

#[derive(Debug, Error)]
pub enum PriceError {
    #[error("Deposit is missing a basis lookup entry for TxId `{0}`")]
    DepositNeedsBasis(String),

    #[error("{0} is missing an exchange rate for TxId `{1}`, suggested exchange rate: {2}")]
    WalletNeedsBasisSuggestion(&'static str, String, UsdAmount),

    #[error("{0} is missing an exchange rate for TxId `{1}` at {2}")]
    WalletNeedsBasis(&'static str, String, DateTime<Utc>),

    #[error("KrakenAmount conversion error")]
    ConvertAmount(#[from] crate::errors::ConvertAmountError),

    #[error("PoolAsset error")]
    PoolAsset(#[from] crate::errors::SplitBasisError),

    #[error("Exchange rate error")]
    ExchangeRate(#[from] ExchangeRateError),

    #[error("Multiple exchange rate errors:{}", print_exchange_rate_errors(.0))]
    ExchangeRates(Vec<ExchangeRateError>),

    #[error("Trades CSV is missing exchange rate for RefId `{0}`")]
    Trades(String),

    #[error("Blockchain Tx error for TxId `{0}`")]
    BlockchainTx(String, #[source] crate::errors::BlockchainError),

    #[error("Unable to process pending withdrawal for TxId `{0}`")]
    PendingWithdrawal(String, #[source] crate::errors::BlockchainError),

    #[error("Unable to process spend for TxId `{0}`")]
    Spend(String, #[source] crate::errors::BlockchainError),

    #[error("Loan error: `{0}`")]
    Loan(String, #[source] crate::errors::BlockchainError),
}

fn print_exchange_rate_errors(errors: &[ExchangeRateError]) -> String {
    let mut output = String::new();

    for err in errors {
        write!(&mut output, "\n  - {err}").unwrap();
        for source in err.sources().skip(1) {
            write!(&mut output, "\n    Caused by {source}").unwrap();
        }
    }

    output
}

/// All taxable events resolved by [`State::resolve`].
///
/// This type is "partial" because it contains a mixture of successfully resolved events and errors
/// raised during resolution. This can be finalized by passing it to [`CheckList::execute`].
///
/// [`CheckList::execute`]: crate::basis::CheckList::execute
#[derive(Debug)]
pub struct Events {
    pub(crate) inner: Vec<Result<Event, PriceError>>,
}

#[derive(Clone, Debug)]
pub(crate) struct SplittableTakeWhile<A: Sized> {
    // want to add some constraint on A that it's Splittable<A>, but this is circular.  [rgrant 20220811 01:13 UTC]
    pub(crate) takes: Vec<A>,
    pub(crate) remain: Option<A>, // empty if exact fit into takes
}

impl<A: Sized> SplittableTakeWhile<A> {
    pub(crate) fn remain(&mut self) -> Option<A> {
        self.remain.take()
    }
}

/// This is just a little container for common function arguments used in this module.
#[derive(Debug)]
struct Args {
    gain_config: GainConfig,
    trades: HashMap<String, KrakenAmount>,

    // Pending spends/withdrawals are not part of `State` because we do not need to persist them.
    // They are collected while processing CSV rows, and processed at the end in a second pass.
    pending_spends: Pending,
    pending_withdrawals: Pending,

    basis_lookup: BasisLookup,
}

/// Given a list of wallets and ledgers, return the earliest timestamp found.
///
/// Returns the current timestamp when both slices are empty.
fn next_timestamp<'a, W, L>(wallets: W, ledgers: L) -> DateTime<Utc>
where
    W: Iterator<Item = &'a FIFO<Tx>>,
    L: Iterator<Item = &'a FIFO<LedgerParsed>>,
{
    wallets
        .flat_map(|wallet| wallet.peek_front().map(|tx| tx.time))
        .chain(ledgers.flat_map(|ledger| ledger.peek_front().map(|lp| lp.get_time())))
        .fold(Utc::now(), |start_time, time| start_time.min(time))
}

/// Take a timeslice from the ledger FIFO bounded by `end_time`.
///
/// Consumes from the front of the FIFO while the ledger timestamp is earlier than or equal to
/// `end_time`.
impl FIFO<LedgerParsed> {
    fn take_timeslice(&mut self, end_time: DateTime<Utc>) -> Vec<LedgerParsed> {
        let mut output = vec![];

        while let Some(lp) = self.peek_front() {
            if lp.get_time() > end_time {
                break;
            }

            output.push(self.pop_front().unwrap());
        }

        output
    }
}

/// Take a timeslice from the wallet FIFO bounded by `end_time`.
///
/// Consumes from the front of the FIFO while the wallet tx timestamp is earlier than or equal to
/// `end_time`.
impl FIFO<Tx> {
    fn take_timeslice(&mut self, end_time: DateTime<Utc>) -> Vec<Tx> {
        let mut output = vec![];

        while let Some(tx) = self.peek_front() {
            if tx.time > end_time {
                break;
            }

            output.push(self.pop_front().unwrap());
        }

        output
    }
}

// TODO: Replace the macro with a generic function, if it is even possible to pass the `inputs`
// iterator that yields different types.

/// This is the inner implementation for `State::match_one_tx`. It is a macro for quick-and-dirty
/// duck typing.
///
/// # Notes on transfers and categorization
///
/// If the asset is sent to an exchange address, it needs to become a pending deposit. If it is
/// received from an exchange address, it will match some pending withdrawal. If the receive
/// address matches one of our wallets, we just move the cost basis.
///
/// If the asset is received from any unknown address, it is one of (non-exhaustively):
///
/// 1. Income.
/// 2. Withdrawal from exchange.
/// 3. Mining rewards.
/// 4. Promotional airdrop.
/// 5. A tax-deductible gift.
///
/// If the asset is sent to any unknown address, it is one of (non-exhaustively):
///
/// 1. Spend with capital gain.
/// 2. Deposit to exchange.
/// 3. A tax-deductible charity donation.
macro_rules! match_one_tx_inner {
    (
        $self:ident,
        $worksheet_name:ident,
        $tx:ident,
        $inputs:ident,
        $loan_inputs:ident,
        $outputs:ident,
        $asset_type:ident,
        $asset_field:ident,
        $blockchain_type:ident,
        $blockchain_field:ident,
        $args:ident,
    ) => {{
        let mut events = vec![];
        // These conditions are named with the perspective of the wallet in mind. For instance, when
        // the transaction outputs are "mine" (the public key belongs to one of my wallets) then we
        // consider the transaction a receive. If the inputs are mine, it's a spend. If both inputs
        // and outputs are mine, it's a transfer.
        let is_spend = !$tx.ins.is_empty() && $tx.ins.iter().all(|txi| txi.mine);
        let is_receive = !is_spend && !$tx.outs.is_empty() && $tx.outs.iter().any(|txo| txo.mine);
        let is_transfer = is_spend && !$tx.outs.is_empty() && $tx.outs.iter().all(|txo| txo.mine);

        let exchange_rate = $tx.exchange_rate.and_then(|exchange_rate| {
            exchange_rate
                .get_exchange_rate($tx.time, &$args.gain_config.exchange_rates_db)
                .ok()
                .map(|asset_exchange_rate_in_usd| {
                    exchange_rate.get_value_usd(asset_exchange_rate_in_usd)
                })
        });

        let internal_account = Vec::from_iter(HashSet::<&str>::from_iter(
            $tx.outs.iter().flat_map(|txo| txo.accounts()),
        ))
        .join(", ");
        let note = Vec::from_iter(HashSet::<&str>::from_iter(
            $tx.outs.iter().flat_map(|txo| txo.notes()),
        ))
        .join(", ");

        let dir = if is_transfer {
            WalletDirection::Move
        } else if is_receive {
            WalletDirection::Receive
        } else {
            WalletDirection::Send
        };

        let incoming = $outputs
            .clone()
            .filter_map(|(txid, amount, mine)| mine.then_some((txid, amount)));
        let amount = incoming
            .clone()
            .fold($asset_type::default(), |acc, (_, amount)| {
                acc + amount.try_into().unwrap()
            });

        let loan_needs_basis = || -> PriceError {
            match $args
                .gain_config
                .exchange_rates_db
                .get(amount.into(), $tx.time)
            {
                Ok(suggested) => {
                    PriceError::WalletNeedsBasisSuggestion("Loan", $tx.txid.clone(), suggested)
                }
                Err(_) => PriceError::WalletNeedsBasis("Loan", $tx.txid.clone(), $tx.time),
            }
        };

        let mut event = Event::from_transaction(
            $worksheet_name.clone(),
            dir,
            &$tx.tx_type,
            $tx.time,
            internal_account.clone(),
            note.clone(),
            $tx.txid.clone(),
        );

        if is_transfer {
            // Handle known moves between own wallets.

            let fee = match $self
                .on_chain_balances
                .$asset_field
                .transfer($inputs, $outputs)
            {
                Ok(fifo) => fifo,
                Err(err) => {
                    return vec![Err(PriceError::BlockchainTx($tx.txid.clone(), err.into()))];
                }
            };

            // No defined rate covers the fee asset: the market rate is used.
            let (errors, _fee_value) = event.add_fee(
                fee.into_iter()
                    .map(|asset| asset.to_non_splittable())
                    .collect(),
                None,
                &$args.gain_config,
            );
            if !errors.is_empty() {
                return errors.into_iter().map(|err| Err(err.into())).collect();
            }

            events.push(Ok(event));
        } else if is_receive {
            // Handle receives from unknown addresses.

            // This needs to do two things:
            // 1. Check `pending_withdrawals` for any matching withdrawal and move the basis
            //    lifecycles into `on_chain_balances` (UTXOs).
            // 2. If no pending withdrawals match heuristically, treat the receive as income and
            //    produce a taxable event. The cost basis needs to be provided manually in the
            //    basis lookup CSV.
            //
            // The fee is paid by the sender (on the blockchain) so we discard the fee when
            // handling receives. Exchange withdrawals have their own fees and those are managed
            // by `handle_withdrawal` when the pending withdrawal is posted.

            let search_start = $tx.time - HEURISTIC_SEARCH_WINDOW_BLOCKWAIT;
            let search_end = $tx.time + HEURISTIC_SEARCH_WINDOW_MISORDERED;
            let fifo = $args
                .pending_withdrawals
                .$asset_field
                .extract_deposit(search_start..=search_end, amount);

            use LoanRole::*;
            use TxType::*;

            if let Some(
                LoanCapital {
                    role: role @ Borrower,
                    loan_id,
                }
                | LoanCollateral {
                    role: role @ Lender,
                    loan_id,
                },
            ) = &$tx.tx_type
            {
                // Borrowing loan capital; create cost basis with the declared exchange rate.
                //
                // ... OR ...
                //
                // Lender is assigned loan collateral; create cost basis with the declared exchange
                // rate.

                let basis: FIFO<_> = match FIFO::try_from_tx(&$tx, exchange_rate) {
                    Some(basis) => basis,
                    None => return vec![Err(loan_needs_basis())],
                };

                if let Err(err) = $self
                    .on_chain_balances
                    .$asset_field
                    .receive(incoming, basis)
                {
                    return vec![Err(PriceError::Loan(
                        match role {
                            Borrower => format!("Borrowing loan capital for `{loan_id}`"),
                            Lender => format!("Lender is assigned loan collateral for `{loan_id}`"),
                        },
                        err.into(),
                    ))];
                }
            } else if let Some(LoanCapital {
                role: Lender,
                loan_id,
            }) = &$tx.tx_type
            {
                // Receiving repayment of loan capital.

                // Move cost basis from state.lender_capital. We don't pay the fee for receives, but
                // we technically want to "spend" from the suspended state to all Tx outputs.
                let outputs = $outputs.filter(|(_txid, _amount, mine)| *mine);
                let (_, txos) =
                    match $self
                        .lender_capital
                        .$asset_field
                        .spend($loan_inputs, outputs, false)
                    {
                        Ok(output) => output,
                        Err(err) => {
                            return vec![Err(PriceError::Loan(
                                format!("Receiving repayment of loan capital for `{loan_id}`"),
                                err.into(),
                            ))];
                        }
                    };
                $self.on_chain_balances.$asset_field.extend(txos);
            } else if let Some(LoanCollateral {
                role: Borrower,
                loan_id,
            }) = &$tx.tx_type
            {
                // Receiving returned loan collateral.

                // Move cost basis from state.borrower_collateral. We don't pay the fee for
                // receives, but we technically want to "spend" from the suspended state to all Tx
                // outputs.
                let outputs = $outputs.filter(|(_txid, _amount, mine)| *mine);
                let (_, txos) =
                    match $self
                        .borrower_collateral
                        .$asset_field
                        .spend($loan_inputs, outputs, false)
                    {
                        Ok(output) => output,
                        Err(err) => {
                            return vec![Err(PriceError::Loan(
                                format!("Receiving returned loan collateral for `{loan_id}`"),
                                err.into(),
                            ))];
                        }
                    };
                $self.on_chain_balances.$asset_field.extend(txos);
            } else if let Some((_txid, _time, basis)) = fifo {
                // Move cost basis (split assets) into on-chain balances.
                if let Err(err) = $self
                    .on_chain_balances
                    .$asset_field
                    .receive(incoming, basis)
                {
                    return vec![Err(PriceError::PendingWithdrawal(
                        $tx.txid.clone(),
                        err.into(),
                    ))];
                }
            } else {
                // Create taxable event for income.

                // TODO: Disambiguate between:
                //
                // - income (receive coins in exchange for work)
                //   - Currently this is the only case we handle.
                // - capital gain/loss (receive coins in exchange for a tangible item, and the
                //   exchange of these items is not your primary business),
                // - trade for another asset (e.g. Selling ETH for BTC).
                // - loan (borrowed capital)
                // - fork (network hard forks and your existing coins gain value on the new
                //   network, this is similar to owning a cow and it gives birth to a calf)

                let wallet_needs_basis = || -> PriceError {
                    match $args
                        .gain_config
                        .exchange_rates_db
                        .get(amount.into(), $tx.time)
                    {
                        Ok(suggested) => PriceError::WalletNeedsBasisSuggestion(
                            "Income",
                            $tx.txid.clone(),
                            suggested,
                        ),
                        Err(_) => {
                            PriceError::WalletNeedsBasis("Income", $tx.txid.clone(), $tx.time)
                        }
                    }
                };

                // If we have an exchange rate on the Tx, then that should cover the basis. Or
                // suggest an exchange rate using the exchange rates DB through an error message.
                let basis: FIFO<PoolAsset<$asset_type>> =
                    match FIFO::try_from_tx(&$tx, exchange_rate) {
                        Some(basis) => basis,
                        None => return vec![Err(wallet_needs_basis())],
                    };

                // Set the event's incoming asset exchange rate, or return an error if the Tx does
                // not have a defined exchange rate.
                event.event_info.asset_in_exchange_rate = match exchange_rate {
                    Some(exchange_rate) => Some(exchange_rate),
                    None => return vec![Err(wallet_needs_basis())],
                };

                // Set the event's proceeds.
                event.event_info.proceeds =
                    KrakenAmount::from(basis.amount()).get_value_usd(exchange_rate.unwrap());

                let errors = event.add_income(basis.iter());
                if !errors.is_empty() {
                    return errors.into_iter().map(|err| Err(err.into())).collect();
                }

                // Move cost basis (split assets) into on-chain balances.
                if let Err(err) = $self
                    .on_chain_balances
                    .$asset_field
                    .receive(incoming, basis)
                {
                    return vec![Err(PriceError::BlockchainTx($tx.txid.clone(), err.into()))];
                }

                events.push(Ok(event));
            }
        } else if is_spend {
            // Handle spends to unknown addresses.

            let (mut fees, mut txos) = match $self
                .on_chain_balances
                .$asset_field
                .spend($inputs, $outputs, true)
            {
                Ok(output) => output,
                Err(err) => {
                    return vec![Err(PriceError::Spend($tx.txid.clone(), err.into()))];
                }
            };

            use LoanRole::*;
            use TxType::*;

            if matches!(
                $tx.tx_type,
                Some(LoanCapital { .. } | LoanCollateral { .. })
            ) {
                // The declared rate covers the fee asset.
                event.event_info.asset_out_exchange_rate = exchange_rate;

                // Handle the fee
                let (errors, fee_value) = event.add_fee(
                    fees.into_iter()
                        .map(|asset| asset.to_non_splittable())
                        .collect(),
                    exchange_rate,
                    &$args.gain_config,
                );
                if !errors.is_empty() {
                    return errors.into_iter().map(|err| Err(err.into())).collect();
                }

                if let Some(
                    LoanCapital { role: Borrower, .. } | LoanCollateral { role: Lender, .. },
                ) = &$tx.tx_type
                {
                    // Returning loan capital; This comes out of the user's wallet balances and
                    // could have a taxable event for any gains or losses accrued during the term
                    // of the loan.
                    //
                    // ... OR ...
                    //
                    // Returning loan collateral. This comes out of the user's wallet balances and
                    // could have a taxable event for any gains or losses accrued during the term
                    // of the loan.

                    // TODO: The basis on the asset sent is compared to the exchange rate on the
                    // loan term, _not_ the Fair Market Value. For now, we require the exchange
                    // rate be defined in both places (receives and returns), but we don't check
                    // that they match.

                    for (_txid, basis) in txos.drain() {
                        event.event_info.proceeds += match exchange_rate {
                            Some(exchange_rate) => {
                                KrakenAmount::from(basis.amount()).get_value_usd(exchange_rate)
                            }
                            None => return vec![Err(loan_needs_basis())],
                        };
                        let split_assets = basis
                            .into_iter()
                            .map(|pool_asset| pool_asset.to_non_splittable())
                            .collect();
                        let errors = event.add_trade(split_assets, &$args.gain_config);
                        if !errors.is_empty() {
                            return vec![Err(PriceError::ExchangeRates(errors))];
                        }
                    }

                    // Trade fees offset the trade atoms' proceeds.
                    event.reduce_trade_proceeds(0, fee_value);
                } else if let Some(LoanCapital {
                    role: Lender,
                    loan_id,
                }) = &$tx.tx_type
                {
                    // Lending loan capital; Put the asset's basis into a suspended state where it
                    // cannot be spent until it is paid back by the borrower.

                    // Move cost basis to state.lender_capital.
                    let fifo = txos.drain().flat_map(|(_, fifo)| fifo).collect();
                    $self
                        .lender_capital
                        .$asset_field
                        .extend([(loan_id.to_string(), fifo)].into_iter());
                } else if let Some(LoanCollateral {
                    role: Borrower,
                    loan_id,
                }) = &$tx.tx_type
                {
                    // Sending loan collateral.

                    // Move cost basis to state.borrower_collateral.
                    let fifo = txos.drain().flat_map(|(_, fifo)| fifo).collect();
                    $self
                        .borrower_collateral
                        .$asset_field
                        .extend([(loan_id.to_string(), fifo)].into_iter());
                } else {
                    unreachable!("BUG: Somehow didn't match all loan types");
                }

                events.push(Ok(event));
            } else {
                // We can only truly support spends with one output that is not mine. This invariant
                // needs to hold so that the fee can be directly associated with the spend for tax
                // reporting.
                assert_eq!(txos.len(), 1);

                // TODO: until bootstrap does more input validation, assert here for users.
                //  Gitlab issue #142.
                if AssetName::from(fees.amount()) == AssetName::Btc {
                    assert!(fees.amount() < "0.5".parse().unwrap());
                }

                // Move the resulting UTXOs to `pending_spends`, where they will either be picked
                // up by `handle_deposits` or `cleanup_pending_spends` later.
                $args
                    .pending_spends
                    .$asset_field
                    .entry($tx.time)
                    .and_modify(|entry| {
                        entry
                            .tx_info
                            .extend(HashMap::<String, _>::from_iter(txos.iter().map(
                                |(txid, _)| {
                                    let value = PendingTxInfo {
                                        exchange_rate,
                                        dir,
                                        tx_type: $tx.tx_type.clone(),
                                        account_name: internal_account.clone(),
                                        note: note.clone(),
                                        fees: fees.drain(..).collect(),
                                    };

                                    (txid.clone(), value)
                                },
                            )));

                        entry.$blockchain_field.extend(txos.drain());
                    })
                    .or_insert_with(|| {
                        let tx_info = HashMap::from_iter(txos.iter().map(|(txid, _)| {
                            let value = PendingTxInfo {
                                exchange_rate,
                                dir,
                                tx_type: $tx.tx_type.clone(),
                                account_name: internal_account.clone(),
                                note: note.clone(),
                                fees: fees.drain(..).collect(),
                            };

                            (txid.clone(), value)
                        }));

                        $blockchain_type {
                            worksheet_name: $worksheet_name.clone(),
                            $blockchain_field: txos,
                            tx_info,
                        }
                    });
            }
        } else {
            unreachable!("BUG: not a transfer, receive, or spend");
        }

        events
    }};
}

impl State {
    /// Resolve a list of [`Tx`] FIFOs and a list of [`LedgerParsed`] FIFOs into a list of
    /// Event-Results. These input FIFOs must each be chronologically ordered.
    ///
    /// The list of [`TradeRow`] is used to create a `TxId -> ExchangeRate` mapping for the ledger.
    ///
    /// The results need to be unwrapped later, which can provide a complete "checklist" of all info
    /// that was missing when the resolve was attempted.
    ///
    /// When an asset is bought it is considered "incoming", and when it is sold it is considered
    /// "outgoing". Fees are always outgoing. Credits are incoming, debits are outgoing, and so on.
    pub fn resolve(
        &mut self,
        mut wallets: HashMap<AssetName, FIFO<Tx>>,
        mut ledgers: HashMap<Rc<str>, FIFO<LedgerParsed>>,
        gain_config: GainConfig,
        trades: FIFO<TradeRow>,
        basis_lookup: FIFO<BasisRow>,
    ) -> Events {
        let trades = trades
            .into_iter()
            .map(|row| (row.txid, row.price))
            .collect();
        let mut args = Args {
            gain_config,
            trades,
            pending_spends: Pending::default(),
            pending_withdrawals: Pending::default(),
            basis_lookup: basis_lookup.parse(),
        };

        let mut events = vec![];

        // The start time will be the earliest timestamp in any wallet or ledger.
        let mut time = next_timestamp(wallets.values(), ledgers.values());

        // One minute seems like a good enough timeslice, but it can be adjusted as necessary.
        let timeslice = Duration::from_secs(60);

        // This loop acts as a scheduler. It consumes a small timeslice from each of the `wallets`
        // and `ledgers` FIFOs, and advances `self`.
        let mut more_work = true;
        while more_work {
            // Assume there's nothing left to do.
            more_work = false;

            // Get the next timeslice, skipping over long sections of downtime.
            let end_time = time.max(next_timestamp(wallets.values(), ledgers.values())) + timeslice;

            // Process a timeslice of all ledgers.
            for (worksheet_name, ledger) in ledgers.iter_mut() {
                for lp in ledger.take_timeslice(end_time) {
                    events.extend(self.match_one_trade(worksheet_name, lp, &mut args));
                }
                trace!(
                    "Ledger {worksheet_name} has {} rows remaining",
                    ledger.len(),
                );

                // Check if there is more work left.
                more_work |= !ledger.is_empty();
            }

            // Process a timeslice of all wallets.
            for (asset, wallet) in wallets.iter_mut() {
                let worksheet_name = Rc::from(format!("{asset}-wallet").as_str());
                for tx in wallet.take_timeslice(end_time) {
                    events.extend(self.match_one_tx(&worksheet_name, tx, &mut args));
                }
                trace!(
                    "Wallet {worksheet_name} has {} rows remaining",
                    wallet.len(),
                );

                // Check if there is more work left.
                more_work |= !wallet.is_empty();
            }

            // Step the clock forward one timeslice.
            time = end_time;
        }

        // Cleanup pending spends.
        events.extend(cleanup_pending_spends(
            args.pending_spends.btc,
            &args.gain_config,
        ));
        events.extend(cleanup_pending_spends(
            args.pending_spends.eth,
            &args.gain_config,
        ));
        events.extend(cleanup_pending_spends(
            args.pending_spends.ethw,
            &args.gain_config,
        ));
        events.extend(cleanup_pending_spends(
            args.pending_spends.usdc,
            &args.gain_config,
        ));
        events.extend(cleanup_pending_spends(
            args.pending_spends.usdt,
            &args.gain_config,
        ));

        // Cleanup pending withdrawals.
        cleanup_pending_withdrawals(
            &mut self.pending_withdrawals.btc,
            args.pending_withdrawals.btc,
        );
        cleanup_pending_withdrawals(
            &mut self.pending_withdrawals.eth,
            args.pending_withdrawals.eth,
        );
        cleanup_pending_withdrawals(
            &mut self.pending_withdrawals.ethw,
            args.pending_withdrawals.ethw,
        );
        cleanup_pending_withdrawals(
            &mut self.pending_withdrawals.usdc,
            args.pending_withdrawals.usdc,
        );
        cleanup_pending_withdrawals(
            &mut self.pending_withdrawals.usdt,
            args.pending_withdrawals.usdt,
        );

        // Update latest row time (seems to be for debugging only).
        self.header.latest_row_time = events
            .iter()
            .filter_map(|res| res.as_ref().ok())
            .next_back()
            .map(|m| m.get_row_time().to_string());

        Events { inner: events }
    }

    /// This is the entry point for handling exchange ledger log lines. Each line has already been
    /// preprocessed (aka "Parsed" as in `LedgerParsed`). This method delegates to other methods
    /// based on the type of log line it matches.
    fn match_one_trade(
        &mut self,
        worksheet_name: &Rc<str>,
        lp: LedgerParsed,
        args: &mut Args,
    ) -> Vec<Result<Event, PriceError>> {
        debug!("Matching: {lp:?}");
        match lp {
            LedgerParsed::Trade { .. } | LedgerParsed::MarginPositionSettle { .. } => {
                self.handle_trade(worksheet_name, Rc::new(lp), args)
            }
            LedgerParsed::MarginPositionOpen(_) => {
                self.handle_margin_open(worksheet_name, Rc::new(lp), args)
            }
            LedgerParsed::MarginPositionRollover(_) => {
                self.handle_margin_rollover(worksheet_name, Rc::new(lp), args)
            }
            LedgerParsed::MarginPositionClose { .. } => {
                self.handle_margin_close(worksheet_name, Rc::new(lp), args)
            }
            LedgerParsed::Deposit(_) => {
                match self.handle_deposit(worksheet_name, Rc::new(lp), args) {
                    Err(err) => vec![Err(err)],
                    Ok(Some(event)) => vec![Ok(event)],
                    Ok(None) => vec![],
                }
            }
            LedgerParsed::Withdrawal(_) => {
                self.handle_withdrawal(worksheet_name, Rc::new(lp), args)
            }
        }
    }

    fn handle_trade(
        &mut self,
        worksheet_name: &Rc<str>,
        lp: Rc<LedgerParsed>,
        args: &mut Args,
    ) -> Vec<Result<Event, PriceError>> {
        if let LedgerParsed::Trade { row_out, row_in }
        | LedgerParsed::MarginPositionSettle { row_out, row_in } = &*lp
        {
            // TODO: Find a way to hoist the Event out of these inner functions...
            let mut event = Event::from_ledger_parsed(
                worksheet_name.clone(),
                lp.clone(),
                row_out.time,
                row_out.refid.clone(),
                row_out.txid.clone(),
            );

            if !matches!(row_out.amount, KrakenAmount::Usd(_)) {
                // The definitional exchange rate is only needed for the outgoing asset.
                let (a, b) = (row_in.amount, row_out.amount.abs());

                let asset_in_exchange_rate =
                    match a.get_exchange_rate(row_in.time, &args.gain_config.exchange_rates_db) {
                        Ok(exchange_rate) => exchange_rate,
                        Err(err) => return vec![Err(err.into())],
                    };

                let asset_out_exchange_rate = match args.trades.get(&row_out.refid) {
                    Some(price) => {
                        let price = match get_asset_pair(a.get_asset(), b.get_asset()) {
                            (_, Trade::Buy) => *price,
                            (_, Trade::Sell) => price.inverse(),
                        };

                        // This correctly gets the cross rate.
                        // E.g. ETHBTC has a price denominated in BTC (the quote asset) and the
                        // exchange rates DB has been used to lookup the ETHUSD price, denominated
                        // in its quote asset, USD.
                        price.get_value_usd(asset_in_exchange_rate)
                    }
                    None => return vec![Err(PriceError::Trades(row_out.refid.to_string()))],
                };

                debug!("handle_trade() asset_in_exchange_rate: {asset_in_exchange_rate:?}");
                debug!(
                    "handle_trade() definitional asset_out_exchange_rate: {asset_out_exchange_rate:?}"
                );

                event.event_info.asset_out_exchange_rate = Some(asset_out_exchange_rate);
                event.event_info.asset_in_exchange_rate = Some(asset_in_exchange_rate);
                event.event_info.proceeds = b.get_value_usd(asset_out_exchange_rate);
            };

            if row_in.amount.is_positive() {
                let tworow = LedgerTwoRowTrade {
                    row_out: row_out.clone(),
                    row_in: row_in.clone(),
                };
                let lifecycle = BasisLifecycle::lifecycle_from_trade_buy(tworow);

                self.acquire_poolasset(row_in.amount, lifecycle);
            }

            // Handle inputs and outputs separately.
            //
            // A fiat-denominated buy capitalizes the fee into the acquired split's basis (see
            // `BasisLifecycle::get_exchange_rate_at_acquisition`), so the fee is not released
            // as a fee atom.
            let row_out_fee =
                if row_in.amount.is_positive() && matches!(row_out.amount, KrakenAmount::Usd(_)) {
                    KrakenAmount::Usd(FiatAmount::default())
                } else {
                    -row_out.fee
                };

            // The event's defined rate covers the outgoing (row_out) asset, which is also the
            // asset the row_out fee is denominated in. It does not cover a row_in fee, which
            // falls back to the market rate.
            let asset_fee = (row_out_fee, event.event_info.asset_out_exchange_rate);
            let mut errors = self.release_poolasset(&mut event, args, row_out.amount, asset_fee);
            let input_errors =
                self.release_poolasset(&mut event, args, row_in.amount, (-row_in.fee, None));

            errors.extend(input_errors);

            if errors.is_empty() {
                vec![Ok(event)]
            } else {
                errors.into_iter().map(Err).collect()
            }
        } else {
            unreachable!()
        }
    }

    // TODO: The only difference between these three functions is the assertion.
    fn handle_margin_open(
        &mut self,
        worksheet_name: &Rc<str>,
        lp: Rc<LedgerParsed>,
        args: &mut Args,
    ) -> Vec<Result<Event, PriceError>> {
        if let LedgerParsed::MarginPositionOpen(lrt) = &*lp {
            debug!("handle_margin_open() lrt: {lrt:?}");

            assert!(lrt.amount.is_zero() || matches!(lrt.amount, KrakenAmount::Usd(_)));

            let mut event = Event::from_ledger_parsed(
                worksheet_name.clone(),
                lp.clone(),
                lrt.time,
                lrt.refid.clone(),
                lrt.txid.clone(),
            );

            // No defined rate covers the fee asset: the market rate is used.
            let errors = self.release_poolasset(&mut event, args, lrt.amount, (-lrt.fee, None));

            if errors.is_empty() {
                vec![Ok(event)]
            } else {
                errors.into_iter().map(Err).collect()
            }
        } else {
            unreachable!()
        }
    }

    fn handle_margin_rollover(
        &mut self,
        worksheet_name: &Rc<str>,
        lp: Rc<LedgerParsed>,
        args: &mut Args,
    ) -> Vec<Result<Event, PriceError>> {
        if let LedgerParsed::MarginPositionRollover(lrt) = &*lp {
            debug!("handle_margin_rollover() lrt: {lrt:?}");

            assert!(lrt.amount.is_zero());

            let mut event = Event::from_ledger_parsed(
                worksheet_name.clone(),
                lp.clone(),
                lrt.time,
                lrt.refid.clone(),
                lrt.txid.clone(),
            );

            // No defined rate covers the fee asset: the market rate is used.
            let errors = self.release_poolasset(&mut event, args, lrt.amount, (-lrt.fee, None));

            if errors.is_empty() {
                vec![Ok(event)]
            } else {
                errors.into_iter().map(Err).collect()
            }
        } else {
            unreachable!()
        }
    }

    fn handle_withdrawal(
        &mut self,
        worksheet_name: &Rc<str>,
        lp: Rc<LedgerParsed>,
        args: &mut Args,
    ) -> Vec<Result<Event, PriceError>> {
        if let LedgerParsed::Withdrawal(lrt) = &*lp {
            debug!("handle_withdrawal() lrt: {lrt:?}");

            assert!(lrt.amount.is_negative());
            assert!(lrt.fee.is_positive() || lrt.fee.is_zero());

            // Create a taxable event for the withdrawal fee.
            let mut event = Event::from_ledger_parsed(
                worksheet_name.clone(),
                lp.clone(),
                lrt.time,
                lrt.refid.clone(),
                lrt.txid.clone(),
            );

            // No defined rate covers the fee asset: the market rate is used.
            let errors = self.release_poolasset(&mut event, args, lrt.amount, (-lrt.fee, None));

            if errors.is_empty() {
                vec![Ok(event)]
            } else {
                errors.into_iter().map(Err).collect()
            }
        } else {
            unreachable!()
        }
    }

    fn handle_margin_close(
        &mut self,
        worksheet_name: &Rc<str>,
        lp: Rc<LedgerParsed>,
        args: &mut Args,
    ) -> Vec<Result<Event, PriceError>> {
        if let LedgerParsed::MarginPositionClose {
            row_proceeds,
            row_fee,
            exchange_rate,
        } = &*lp
        {
            // The assumption here is that closing a position has only a fee on the second row.
            assert!(row_fee.amount.is_zero());
            assert!(row_fee.fee.is_zero() || row_fee.fee.is_positive());

            let mut errors = vec![];
            let mut event = Event::from_ledger_parsed(
                worksheet_name.clone(),
                lp.clone(),
                row_proceeds.time,
                row_proceeds.refid.clone(),
                row_proceeds.txid.clone(),
            );

            // We need the asset-in exchange rate to calculate proceeds as USD.
            // E.g. EUR is about $1.10, and USD is exactly $1.00.
            // *IGNORE* the `exchange_rate` for the trade pair from the `MarginPositionClose`.
            match row_proceeds
                .amount
                .get_exchange_rate(row_proceeds.time, &args.gain_config.exchange_rates_db)
            {
                Ok(exchange_rate) => {
                    debug!("handle_margin_close() exchange_rate: {exchange_rate:?}");
                    event.event_info.asset_in_exchange_rate = Some(exchange_rate);
                    event.event_info.proceeds = row_proceeds.amount.get_value_usd(exchange_rate);

                    // The position table is only used for debugging.
                    if !row_proceeds.amount.is_zero() {
                        event.add_position(row_proceeds.amount, &args.gain_config);
                    }
                }
                Err(err) => errors.push(err.into()),
            }

            if row_proceeds.amount.is_positive() {
                let lmc = LedgerMarginClose {
                    row_proceeds: row_proceeds.clone(),
                    row_fee: row_fee.into(),
                    exchange_rate: *exchange_rate,
                };
                let lifecycle = BasisLifecycle::lifecycle_from_margin_close(lmc);

                self.acquire_poolasset(row_proceeds.amount, lifecycle);
            }

            // No defined rate covers the fee asset: the market rate is used.
            let asset_fee = (-row_proceeds.fee, None);
            let errs = self.release_poolasset(&mut event, args, row_proceeds.amount, asset_fee);
            errors.extend(errs);

            let asset_fee = (-row_fee.fee, None);
            let errs = self.release_poolasset(&mut event, args, row_fee.amount, asset_fee);
            errors.extend(errs);

            if errors.is_empty() {
                vec![Ok(event)]
            } else {
                errors.into_iter().map(Err).collect()
            }
        } else {
            unreachable!()
        }
    }

    fn handle_deposit(
        &mut self,
        worksheet_name: &Rc<str>,
        lp: Rc<LedgerParsed>,
        args: &mut Args,
    ) -> Result<Option<Event>, PriceError> {
        if let LedgerParsed::Deposit(lrd) = &*lp {
            debug!("handle_deposit() lrd: {lrd:?}");

            assert!(lrd.amount.is_positive());
            assert!(lrd.fee.is_zero());

            let event = Event::from_ledger_parsed(
                worksheet_name.clone(),
                lp.clone(),
                lrd.time,
                lrd.refid.clone(),
                lrd.txid.clone(),
            );

            let maybe_event = match lrd.amount {
                KrakenAmount::Usd(_) => {
                    // Handle USD as a base currency.
                    // USD deposits don't need the special basis lookup code used by other assets.
                    let pool_asset = PoolAsset::from_base_deposit(lrd);
                    self.exchange_balances.usd.append_back(pool_asset);

                    None
                }
                KrakenAmount::Btc(_) => handle_deposit_inner(
                    event,
                    &mut self.exchange_balances.btc,
                    &mut args.pending_spends.btc,
                    &mut args.basis_lookup.btc,
                    &args.gain_config,
                    lrd,
                )?,
                KrakenAmount::Chf(_) => handle_deposit_inner(
                    event,
                    &mut self.exchange_balances.chf,
                    &mut args.pending_spends.chf,
                    &mut args.basis_lookup.chf,
                    &args.gain_config,
                    lrd,
                )?,
                KrakenAmount::Eth(_) => handle_deposit_inner(
                    event,
                    &mut self.exchange_balances.eth,
                    &mut args.pending_spends.eth,
                    &mut args.basis_lookup.eth,
                    &args.gain_config,
                    lrd,
                )?,
                KrakenAmount::EthW(_) => handle_deposit_inner(
                    event,
                    &mut self.exchange_balances.ethw,
                    &mut args.pending_spends.ethw,
                    &mut args.basis_lookup.ethw,
                    &args.gain_config,
                    lrd,
                )?,
                KrakenAmount::Eur(_) => handle_deposit_inner(
                    event,
                    &mut self.exchange_balances.eur,
                    &mut args.pending_spends.eur,
                    &mut args.basis_lookup.eur,
                    &args.gain_config,
                    lrd,
                )?,
                KrakenAmount::Jpy(_) => handle_deposit_inner(
                    event,
                    &mut self.exchange_balances.jpy,
                    &mut args.pending_spends.jpy,
                    &mut args.basis_lookup.jpy,
                    &args.gain_config,
                    lrd,
                )?,
                KrakenAmount::Usdc(_) => handle_deposit_inner(
                    event,
                    &mut self.exchange_balances.usdc,
                    &mut args.pending_spends.usdc,
                    &mut args.basis_lookup.usdc,
                    &args.gain_config,
                    lrd,
                )?,
                KrakenAmount::Usdt(_) => handle_deposit_inner(
                    event,
                    &mut self.exchange_balances.usdt,
                    &mut args.pending_spends.usdt,
                    &mut args.basis_lookup.usdt,
                    &args.gain_config,
                    lrd,
                )?,
            };

            Ok(maybe_event)
        } else {
            unreachable!()
        }
    }

    /// Acquire a PoolAsset, e.g. a Buy.
    fn acquire_poolasset(&mut self, asset_amount: KrakenAmount, lifecycle: BasisLifecycle) {
        debug!("acquire_poolasset() asset_amount: {asset_amount:?}, lifecycle: {lifecycle:?}");

        assert!(asset_amount.is_positive());

        match asset_amount {
            KrakenAmount::Usd(_) => {
                produce_poolasset(&mut self.exchange_balances.usd, asset_amount, lifecycle);
            }
            KrakenAmount::Btc(_) => {
                produce_poolasset(&mut self.exchange_balances.btc, asset_amount, lifecycle);
            }
            KrakenAmount::Chf(_) => {
                produce_poolasset(&mut self.exchange_balances.chf, asset_amount, lifecycle);
            }
            KrakenAmount::Eth(_) => {
                produce_poolasset(&mut self.exchange_balances.eth, asset_amount, lifecycle);
            }
            KrakenAmount::EthW(_) => {
                produce_poolasset(&mut self.exchange_balances.ethw, asset_amount, lifecycle);
            }
            KrakenAmount::Eur(_) => {
                produce_poolasset(&mut self.exchange_balances.eur, asset_amount, lifecycle);
            }
            KrakenAmount::Jpy(_) => {
                produce_poolasset(&mut self.exchange_balances.jpy, asset_amount, lifecycle);
            }
            KrakenAmount::Usdc(_) => {
                produce_poolasset(&mut self.exchange_balances.usdc, asset_amount, lifecycle);
            }
            KrakenAmount::Usdt(_) => {
                produce_poolasset(&mut self.exchange_balances.usdt, asset_amount, lifecycle);
            }
        }
    }

    /// Release a PoolAsset, e.g. a Sell or Fee.
    ///
    /// `asset_fee` is the fee paid in any asset and its USD rate (if any).
    fn release_poolasset(
        &mut self,
        event: &mut Event,
        args: &mut Args,
        asset_amount: KrakenAmount,
        asset_fee: (KrakenAmount, Option<UsdAmount>),
    ) -> Vec<PriceError> {
        debug!("release_poolasset() event: {event:?}");
        if asset_amount.is_negative() {
            debug!("release_poolasset() asset_amount: {asset_amount:?}");
        }
        if asset_fee.0.is_negative() {
            debug!("release_poolasset() asset_fee: {:?}", asset_fee.0);
        }

        let gain_config = &args.gain_config;

        match (asset_amount, asset_fee.0) {
            (KrakenAmount::Usd(_), KrakenAmount::Usd(_)) => {
                let fifo = &mut self.exchange_balances.usd;
                let utxos = &mut args.pending_withdrawals.usd;
                release_poolasset_inner(event, fifo, utxos, asset_amount, asset_fee, gain_config)
            }
            (KrakenAmount::Btc(_), KrakenAmount::Btc(_)) => {
                let fifo = &mut self.exchange_balances.btc;
                let utxos = &mut args.pending_withdrawals.btc;
                release_poolasset_inner(event, fifo, utxos, asset_amount, asset_fee, gain_config)
            }
            (KrakenAmount::Chf(_), KrakenAmount::Chf(_)) => {
                let fifo = &mut self.exchange_balances.chf;
                let utxos = &mut args.pending_withdrawals.chf;
                release_poolasset_inner(event, fifo, utxos, asset_amount, asset_fee, gain_config)
            }
            (KrakenAmount::Eth(_), KrakenAmount::Eth(_)) => {
                let fifo = &mut self.exchange_balances.eth;
                let utxos = &mut args.pending_withdrawals.eth;
                release_poolasset_inner(event, fifo, utxos, asset_amount, asset_fee, gain_config)
            }
            (KrakenAmount::EthW(_), KrakenAmount::EthW(_)) => {
                let fifo = &mut self.exchange_balances.ethw;
                let utxos = &mut args.pending_withdrawals.ethw;
                release_poolasset_inner(event, fifo, utxos, asset_amount, asset_fee, gain_config)
            }
            (KrakenAmount::Eur(_), KrakenAmount::Eur(_)) => {
                let fifo = &mut self.exchange_balances.eur;
                let utxos = &mut args.pending_withdrawals.eur;
                release_poolasset_inner(event, fifo, utxos, asset_amount, asset_fee, gain_config)
            }
            (KrakenAmount::Jpy(_), KrakenAmount::Jpy(_)) => {
                let fifo = &mut self.exchange_balances.jpy;
                let utxos = &mut args.pending_withdrawals.jpy;
                release_poolasset_inner(event, fifo, utxos, asset_amount, asset_fee, gain_config)
            }
            (KrakenAmount::Usdc(_), KrakenAmount::Usdc(_)) => {
                let fifo = &mut self.exchange_balances.usdc;
                let utxos = &mut args.pending_withdrawals.usdc;
                release_poolasset_inner(event, fifo, utxos, asset_amount, asset_fee, gain_config)
            }
            (KrakenAmount::Usdt(_), KrakenAmount::Usdt(_)) => {
                let fifo = &mut self.exchange_balances.usdt;
                let utxos = &mut args.pending_withdrawals.usdt;
                release_poolasset_inner(event, fifo, utxos, asset_amount, asset_fee, gain_config)
            }
            _ => todo!("Unsupported asset in release_poolasset()"),
        }
    }

    /// This is the entry point for handling wallet transaction log lines. Each line has already
    /// been preprocessed and is represented as a `Tx` transaction.
    fn match_one_tx(
        &mut self,
        worksheet_name: &Rc<str>,
        tx: Tx,
        args: &mut Args,
    ) -> Vec<Result<Event, PriceError>> {
        debug!("Matching: {tx:?}");

        use TxType::*;

        match tx.asset {
            AssetName::Btc => {
                let inputs = tx.ins.iter().map(|txi| &txi.external_id);
                let loan_inputs = match &tx.tx_type {
                    Some(LoanCapital { loan_id, .. } | LoanCollateral { loan_id, .. }) => {
                        Some(loan_id)
                    }
                    _ => None,
                }
                .into_iter();
                let outputs = tx.outs.iter().enumerate().map(|(index, txo)| {
                    (
                        format!("{txid}:{index}", txid = tx.txid),
                        txo.amount,
                        txo.mine,
                    )
                });

                match_one_tx_inner!(
                    self,
                    worksheet_name,
                    tx,
                    inputs,
                    loan_inputs,
                    outputs,
                    BitcoinAmount,
                    btc,
                    PendingUtxo,
                    utxos,
                    args,
                )
            }
            AssetName::Eth => {
                let inputs = tx
                    .ins
                    .iter()
                    .map(|txi| (&txi.external_id, txi.amount.unwrap()));
                let amount = tx
                    .outs
                    .iter()
                    .map(|txo| txo.amount)
                    .fold(KrakenAmount::new("ETH", "0.0").unwrap(), |sum, amount| {
                        sum + amount
                    });
                let loan_inputs = match &tx.tx_type {
                    Some(LoanCapital { loan_id, .. } | LoanCollateral { loan_id, .. }) => {
                        Some((loan_id, amount))
                    }
                    _ => None,
                }
                .into_iter();
                let outputs = tx
                    .outs
                    .iter()
                    .map(|txo| (tx.txid.to_string(), txo.amount, txo.mine));

                match_one_tx_inner!(
                    self,
                    worksheet_name,
                    tx,
                    inputs,
                    loan_inputs,
                    outputs,
                    EtherAmount,
                    eth,
                    PendingAccountTx,
                    account,
                    args,
                )
            }
            AssetName::EthW => {
                let inputs = tx
                    .ins
                    .iter()
                    .map(|txi| (&txi.external_id, txi.amount.unwrap()));
                let amount = tx
                    .outs
                    .iter()
                    .map(|txo| txo.amount)
                    .fold(KrakenAmount::new("ETHW", "0.0").unwrap(), |sum, amount| {
                        sum + amount
                    });
                let loan_inputs = match &tx.tx_type {
                    Some(LoanCapital { loan_id, .. } | LoanCollateral { loan_id, .. }) => {
                        Some((loan_id, amount))
                    }
                    _ => None,
                }
                .into_iter();
                let outputs = tx
                    .outs
                    .iter()
                    .map(|txo| (tx.txid.to_string(), txo.amount, txo.mine));

                match_one_tx_inner!(
                    self,
                    worksheet_name,
                    tx,
                    inputs,
                    loan_inputs,
                    outputs,
                    EthWAmount,
                    ethw,
                    PendingAccountTx,
                    account,
                    args,
                )
            }
            AssetName::Usdc => {
                let inputs = tx
                    .ins
                    .iter()
                    .map(|txi| (&txi.external_id, txi.amount.unwrap()));
                let amount = tx
                    .outs
                    .iter()
                    .map(|txo| txo.amount)
                    .fold(KrakenAmount::new("USDC", "0.0").unwrap(), |sum, amount| {
                        sum + amount
                    });
                let loan_inputs = match &tx.tx_type {
                    Some(LoanCapital { loan_id, .. } | LoanCollateral { loan_id, .. }) => {
                        Some((loan_id, amount))
                    }
                    _ => None,
                }
                .into_iter();
                let outputs = tx
                    .outs
                    .iter()
                    .map(|txo| (tx.txid.to_string(), txo.amount, txo.mine));

                match_one_tx_inner!(
                    self,
                    worksheet_name,
                    tx,
                    inputs,
                    loan_inputs,
                    outputs,
                    UsdcAmount,
                    usdc,
                    PendingAccountTx,
                    account,
                    args,
                )
            }
            AssetName::Usdt => {
                let inputs = tx
                    .ins
                    .iter()
                    .map(|txi| (&txi.external_id, txi.amount.unwrap()));
                let amount = tx
                    .outs
                    .iter()
                    .map(|txo| txo.amount)
                    .fold(KrakenAmount::new("USDT", "0.0").unwrap(), |sum, amount| {
                        sum + amount
                    });
                let loan_inputs = match &tx.tx_type {
                    Some(LoanCapital { loan_id, .. } | LoanCollateral { loan_id, .. }) => {
                        Some((loan_id, amount))
                    }
                    _ => None,
                }
                .into_iter();
                let outputs = tx
                    .outs
                    .iter()
                    .map(|txo| (tx.txid.to_string(), txo.amount, txo.mine));

                match_one_tx_inner!(
                    self,
                    worksheet_name,
                    tx,
                    inputs,
                    loan_inputs,
                    outputs,
                    UsdtAmount,
                    usdt,
                    PendingAccountTx,
                    account,
                    args,
                )
            }
            asset => unimplemented!("Unsupported asset `{asset}`"),
        }
    }
}

fn cleanup_pending_spends<A, B, T>(
    pending_spends: T,
    gain_config: &GainConfig,
) -> Vec<Result<Event, PriceError>>
where
    A: Asset + Copy + Default + Add<Output = A>,
    B: BlockchainExt<Asset = A>,
    T: TimeOrderedBlockchain<Asset = A> + IntoIterator<Item = (DateTime<Utc>, B)>,
    KrakenAmount: From<A>,
    AssetName: From<A>,
{
    pending_spends
        .into_iter()
        .flat_map(|(time, mut spend)| {
            // TODO: I hate that there are two maps that need a rendezvous thingy with key matching.
            // Find a better data model for these pending UTXOs.
            let txos: HashMap<_, _> = spend.ext_drain().collect();
            txos.into_iter()
                .map(|(txid, basis)| {
                    let name = spend.worksheet_name();
                    let tx_info = spend
                        .tx_info(&txid)
                        .expect("Pending spend is missing PendingTxInfo");

                    cleanup_pending_spend(time, name, &txid, tx_info, basis, gain_config)
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn cleanup_pending_spend<A>(
    time: DateTime<Utc>,
    worksheet_name: &Rc<str>,
    txid: &str,
    tx_info: &PendingTxInfo<PoolAsset<A>>,
    basis: FIFO<PoolAsset<A>>,
    gain_config: &GainConfig,
) -> Result<Event, PriceError>
where
    A: Asset + Copy + Default + Add<Output = A>,
    KrakenAmount: From<A>,
    AssetName: From<A>,
{
    // TODO: There is an edge case at the end of the transaction inputs where a spend may in
    // fact be a deposit but we just don't have a ledger entry for it. A concrete example is
    // when a deposit is started on December 31st at 23:59:59 and the exchange doesn't
    // acknowledge the deposit request until January 1st at 00:00:00 ... on the next year! If
    // we are only processing one year worth of CSVs, we won't be able to see the deposit
    // request in the exchange ledger until we process next year's data.
    //
    // Handling this edge case is not terribly difficult, we just need to persist the pending
    // spend as a pending deposit to the checkpoint. The heuristic for this one is whether the
    // spend occurs within some threshold of time at the end of all inputs. Should use the same
    // threshold as `handle_deposit_inner`.
    //
    // Otherwise the _normal_ way to process a pending spend is to produce a taxable event.

    let mut event = Event::from_transaction(
        worksheet_name.clone(),
        tx_info.dir,
        &tx_info.tx_type,
        time,
        tx_info.account_name.clone(),
        tx_info.note.clone(),
        txid.to_string(),
    );

    let wallet_needs_basis = || match gain_config
        .exchange_rates_db
        .get(AssetName::from(A::default()), time)
    {
        Ok(suggested) => {
            PriceError::WalletNeedsBasisSuggestion("Spend", txid.to_string(), suggested)
        }
        Err(_) => PriceError::WalletNeedsBasis("Spend", txid.to_string(), time),
    };

    // Add the exchange rate.
    event.event_info.asset_out_exchange_rate = match tx_info.exchange_rate {
        Some(exchange_rate) => Some(exchange_rate),
        None => return Err(wallet_needs_basis()),
    };

    // Add proceeds for gains error check.
    let exchange_rate = event.event_info.asset_out_exchange_rate.unwrap();
    event.event_info.proceeds = KrakenAmount::from(basis.amount()).get_value_usd(exchange_rate);

    let split_assets = basis
        .into_iter()
        .map(|pool_asset| pool_asset.to_non_splittable())
        .collect();
    let errors = event.add_trade(split_assets, gain_config);
    if !errors.is_empty() {
        return Err(PriceError::ExchangeRates(errors));
    }

    if !tx_info.fees.is_empty() {
        // The declared rate covers the fee asset.
        let (errors, fee_value) = event.add_fee(
            tx_info
                .fees
                .iter()
                .map(|asset| asset.to_non_splittable())
                .collect(),
            event.event_info.asset_out_exchange_rate,
            gain_config,
        );
        if errors.is_empty() {
            // Trade fees offset the trade atoms' proceeds.
            event.reduce_trade_proceeds(0, fee_value);
            Ok(event)
        } else {
            Err(PriceError::ExchangeRates(errors))
        }
    } else {
        Ok(event)
    }
}

fn cleanup_pending_withdrawals<A, B, T>(state: &mut T, pending_withdrawals: T)
where
    A: Asset,
    B: BlockchainExt<Asset = A>,
    T: TimeOrderedBlockchain<Asset = A, Blockchain = B> + IntoIterator<Item = (DateTime<Utc>, B)>,
{
    for (time, withdrawal) in pending_withdrawals.into_iter() {
        cleanup_pending_withdrawal(state, time, withdrawal);
    }
}

fn cleanup_pending_withdrawal<A, B, T>(state: &mut T, time: DateTime<Utc>, withdrawal: B)
where
    A: Asset,
    B: BlockchainExt<Asset = A>,
    T: TimeOrderedBlockchain<Asset = A, Blockchain = B>,
{
    // TODO: See `handle_pending_spend` for documentation of an edge case that we need to take
    // care of.
    state.extend(time, withdrawal);
}

fn handle_deposit_inner<A, B, L, T>(
    mut event: Event,
    fifo: &mut FIFO<PoolAsset<A>>,
    pending_spends: &mut T,
    basis_lookup: &mut L,
    gain_config: &GainConfig,
    lrd: &LedgerRowDeposit,
) -> Result<Option<Event>, PriceError>
where
    A: Asset + Copy + Default + Add<Output = A>,
    B: BlockchainExt<Asset = A>,
    L: BasisLookupExt<Asset = A>,
    T: TimeOrderedBlockchain<Asset = A> + IntoIterator<Item = (DateTime<Utc>, B)>,
    <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
    KrakenAmount: From<A>,
    PriceError: From<<A as TryFrom<KrakenAmount>>::Error>,
{
    // This is an interesting case where a bucket of pending BTCs is needed to create the PoolBTC.
    // These "pending BTCs" are sourced from a list that is different from the `btcs` FIFO.

    // one deposit tx may have several change splits with different basis information

    let search_start = lrd.time - HEURISTIC_SEARCH_WINDOW_BLOCKWAIT;
    let search_end = lrd.time + HEURISTIC_SEARCH_WINDOW_MISORDERED;

    // First attempt to find the basis in the user-specified basis lookup CSV with a fallback to
    // pending spends. This prioritizes the user input over the best-effort heuristic, but it does
    // NOT attempt to resolve duplicates.
    //
    // The heuristic requires that the pending deposit amount exactly matches the ledger deposit
    // amount + fee.
    let amount = (lrd.amount + lrd.fee).try_into()?;
    let (txid, time, basis) = basis_lookup
        .take_basis(&lrd.txid)
        .map(|basis| (lrd.txid.clone(), lrd.time, basis)) // TODO: This is weird but satisfies fee_basis
        .or_else(|| pending_spends.extract_deposit(search_start..=search_end, amount))
        .ok_or_else(|| PriceError::DepositNeedsBasis(lrd.txid.to_string()))?;

    fifo.extend(basis);

    // If pending spend has a fee basis, return a new event.
    if let Some(fee_basis) = pending_spends.fee_basis(time, &txid) {
        // No defined rate covers the fee asset: the market rate is used.
        let (errors, _fee_value) = event.add_fee(
            fee_basis
                .iter()
                .map(|asset| asset.to_non_splittable())
                .collect(),
            None,
            gain_config,
        );
        if errors.is_empty() {
            Ok(Some(event))
        } else {
            Err(PriceError::ExchangeRates(errors))
        }
    } else {
        // The only way to return None is when there is no fee paid on the spend.
        Ok(None)
    }
}

fn release_poolasset_inner<A, B, T>(
    event: &mut Event,
    fifo: &mut FIFO<PoolAsset<A>>,
    pending_withdrawals: &mut T,
    asset_amount: KrakenAmount,
    asset_fee: (KrakenAmount, Option<UsdAmount>),
    gain_config: &GainConfig,
) -> Vec<PriceError>
where
    A: Asset + Copy + Default + Add<Output = A> + Sub<Output = A>,
    B: BlockchainExt<Asset = A>,
    T: TimeOrderedBlockchain<Asset = A, Blockchain = B> + IntoIterator<Item = (DateTime<Utc>, B)>,
    <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
    PoolAsset<A>: PoolAssetSplit<Amount = A>,
    KrakenAmount: From<A>,
{
    let mut errors = vec![];

    // Atoms added before this call must not be modified by the pro-rata proceeds reduction below.
    let trade_atom_start = event.event_details.len();

    if asset_amount.is_negative() {
        match consume_poolasset(fifo, asset_amount) {
            Ok(split_assets) => {
                if event.event_info.asset_out_exchange_rate.is_some()
                    && !matches!(asset_amount, KrakenAmount::Usd(_))
                {
                    let split_assets = split_assets
                        .into_iter()
                        .map(|asset| asset.to_non_splittable())
                        .collect();
                    let trade_errors = event.add_trade(split_assets, gain_config);
                    errors.extend(trade_errors.into_iter().map(|error| error.into()));
                } else if event.is_withdrawal {
                    // Construct a `Utxo` blockchain from the split assets. The `txid:index` does
                    // not matter, as long as it is unique enough. We cannot predict what the
                    // actual TXID on the blockchain is at this point, which is why we need to hold
                    // the basis in a pending-queue.
                    //
                    // Note that the "fees" field on this BlockchainExt will be empty, since we are
                    // handling fees below. It seems consistent to add fees as part of the exchange
                    // withdrawal event, rather than deferring that to an event on the wallet
                    // side.
                    let blockchain = B::ext_new(
                        event.worksheet_name.clone(),
                        [(
                            event.event_info.ledger_row_id.clone(),
                            split_assets.into_iter().collect(),
                        )],
                    );

                    // Create a pending withdrawal for wallet transactions to match against. This
                    // will be used to avoid emitting an income taxable event for receive
                    // transactions.
                    pending_withdrawals.extend(event.event_info.event_date, blockchain);
                }
            }
            Err(error) => errors.push(error),
        }
    }
    if asset_fee.0.is_negative() {
        match consume_poolasset(fifo, asset_fee.0) {
            Ok(split_assets) => {
                let split_assets = split_assets
                    .into_iter()
                    .map(|asset| asset.to_non_splittable())
                    .collect();

                // `fee_rate` covers the fee asset when the event has a defined rate for it.
                // Otherwise `add_fee` falls back to the fee asset's market rate at the event
                // date-time.
                let fee_rate = asset_fee.1;
                let (fee_errors, fee_value) = event.add_fee(split_assets, fee_rate, gain_config);
                errors.extend(fee_errors.into_iter().map(|error| error.into()));

                // Trade fees offset the trade atoms' proceeds added by this same call: the trade
                // atoms' and fee atoms' proceeds must sum to the event's total proceeds.
                event.reduce_trade_proceeds(trade_atom_start, fee_value);
            }
            Err(error) => errors.push(error),
        }
    }

    errors
}

/// This is the generic version of `acquire_poolasset()`.
fn produce_poolasset<A: Asset>(
    fifo: &mut FIFO<PoolAsset<A>>,
    asset_amount: KrakenAmount,
    lifecycle: BasisLifecycle,
) where
    <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
{
    let asset = PoolAsset {
        amount: asset_amount.try_into().unwrap(),
        lifecycle,
    };

    fifo.append_back(asset); // Produce a new PoolAsset into the FIFO.
}

/// This is the generic version of `release_poolasset()`.
fn consume_poolasset<A: Asset + Copy + Default + Add<Output = A> + Sub<Output = A>>(
    fifo: &mut FIFO<PoolAsset<A>>,
    asset_amount: KrakenAmount,
) -> Result<Vec<PoolAsset<A>>, PriceError>
where
    <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
    PoolAsset<A>: PoolAssetSplit<Amount = A>,
{
    let to_take = asset_amount.abs().try_into().unwrap();

    // Consume as many PoolBTCs from the FIFO as needed to fulfill the sell.
    let mut stw = fifo.splittable_take_while(to_take)?;

    // Produce the remainder back onto the FIFO.
    if let Some(remain) = stw.remain() {
        fifo.push_front(remain);
    }

    Ok(stw.takes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::kraken::{read_ledgers, read_trades};
    use crate::imports::wallet::{Txi, Txo, TxoInfo};
    use crate::model::checkpoint::{Balances, CheckpointHeader, UtxoBalances};
    use crate::model::events::{EventAtom, EventSubType, GainPortion, GainTerm};
    use crate::model::exchange_rate::{ExchangeRateMap, ExchangeRates};
    use crate::model::ledgers::rows::{BasisRow, LedgerRowTypical};
    use crate::model::{blockchain::Utxo, CapGainsWorksheet, Stats, Sums};
    use chrono::NaiveDateTime;
    use gitver::GitverHashes;
    use std::collections::{BTreeSet, HashMap};
    use std::rc::Rc;
    use tracing_test::traced_test;

    // use crate::model::{KrakenAmount, Stats, Sums, UsdAmount};

    // Exchange rates are very important for properly reporting taxable events that involve various
    // assets. There are two kinds of taxable events in the US: income and capital gains.
    // (TODO: income reporting is implemented but not tested yet.)
    //
    // Taxable events for capital gains only occur on outgoing assets. A fee is a separate taxable
    // event on the fee coin itself: the fee is booked at its definitional or market value when
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
            event.event_info.proceeds,
            UsdAmount::from("3488.56".parse::<FiatAmount>().unwrap()),
        );

        // The event has two atoms: the trade atom (0.1 BTC) and the fee atom (0.0002 BTC).
        // The trade atom's proceeds are reduced by the fee value, and the fee atom is booked at
        // the event's definitional rate.
        assert_eq!(event.event_details.len(), 2);
        match &event.event_details[0] {
            EventAtom::Trade {
                asset_amount,
                proceeds,
                net_gain,
            } => {
                assert_eq!(asset_amount.to_decimal(), "-0.10000000".parse().unwrap());
                // 3488.56 - (0.0002 * 34885.60)
                assert_eq!(
                    *proceeds,
                    UsdAmount::from("3481.58288".parse::<FiatAmount>().unwrap()),
                );
                match net_gain {
                    GainTerm::LongUs(us) => {
                        let expected =
                            UsdAmount::from("2439.2487127380372".parse::<FiatAmount>().unwrap());
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
                reduces_proceeds,
            } => {
                assert!(*reduces_proceeds);
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
    /// BTC fee. The fee value (300 USD) must be split pro-rata across the two trade atoms (120 and
    /// 180), so the trade atoms' and fee atom's proceeds sum to the event's total proceeds.
    #[test]
    #[traced_test]
    fn test_multi_split_sale_fee_pro_rata_reduction() {
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
            event.event_info.proceeds,
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
                // 0.06 * 100000 - 0.06 * 2000 (pro-rata fee: 300 * 0.06/0.15)
                assert_eq!(
                    *proceeds,
                    UsdAmount::from("5880.00".parse::<FiatAmount>().unwrap()),
                );
                match net_gain {
                    GainTerm::LongUs(us) => {
                        assert_eq!(
                            us.basis,
                            UsdAmount::from("6.00".parse::<FiatAmount>().unwrap()),
                        );
                        assert_eq!(
                            us.net_gain,
                            UsdAmount::from("5874.00".parse::<FiatAmount>().unwrap()),
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
                // 0.09 * 100000 - 180 (remainder of the fee value, absorbed by the last atom)
                assert_eq!(
                    *proceeds,
                    UsdAmount::from("8820.00".parse::<FiatAmount>().unwrap()),
                );
                match net_gain {
                    GainTerm::LongUs(us) => {
                        assert_eq!(
                            us.basis,
                            UsdAmount::from("18.00".parse::<FiatAmount>().unwrap()),
                        );
                        assert_eq!(
                            us.net_gain,
                            UsdAmount::from("8802.00".parse::<FiatAmount>().unwrap()),
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
                reduces_proceeds,
            } => {
                assert!(*reduces_proceeds);
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
        assert_eq!(
            sums.ledger_proceeds(),
            UsdAmount::from("15000.00".parse::<FiatAmount>().unwrap()),
        );
        assert_eq!(
            sums.us_long_trade_proceeds(),
            UsdAmount::from("15000.00".parse::<FiatAmount>().unwrap()),
        );
        assert_eq!(
            sums.us_long_trade_basis(),
            UsdAmount::from("24.60".parse::<FiatAmount>().unwrap()),
        );
        assert_eq!(
            sums.us_long_trade_gain(),
            UsdAmount::from("14975.40".parse::<FiatAmount>().unwrap()),
        );
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
        // The move has one atom: the fee atom (0.03 BTC), booked at the market rate.
        assert_eq!(actual.event_details.len(), 1);
        match &actual.event_details[0] {
            EventAtom::Fee {
                asset_amount,
                proceeds,
                net_gain,
                reduces_proceeds,
            } => {
                assert!(!*reduces_proceeds);
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
                    ).to_string(),
                    concat!(
                        "PoolAsset { amount: BitcoinAmount(0.15000000), lifecycle: BasisLifecycle { ",
                        "synthetic_id: \"feedbeef:0\", resolved_origin: Bucket(Bucket { ",
                        "synthetic_id: \"feedbeef:0\", time: 2014-10-03T12:21:42Z, ",
                        "amount: Btc(BitcoinAmount(0.18000000)), exchange_rate: ",
                        "UsdAmount(FiatAmount(230.82)) }) } }",
                    ).to_string(),
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

    /// Build the worksheet sums for the events, asserting the proceeds invariant.
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
                // 1.00 BTC * $1,000, reduced by the fee value (0.008 BTC * $1,000).
                assert_eq!(*proceeds, usd("992.0000"));
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
                reduces_proceeds,
            } => {
                assert!(*reduces_proceeds);
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
            ("2020-05-15 11:00:00", "10.0800", "981.9200", true),
            ("2026-01-10 10:00:00", "8.0640", "-0.0640", false),
        );

        let db = rates(&["2026-07-20"], &["2026-07-20"]);
        let sums = sums(events);
        assert_eq!(sums.gains_us_long(), usd("981.9200"));
        assert_eq!(sums.gains_us_short(), usd("-0.0640"));

        // Remaining BTC split: 0.992 at $1,008. ETH basis: 20 ETH at $50.
        assert_pool(&state.exchange_balances.btc, &db, "0.99200000", "1008.0000");
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
            ("2026-01-10 10:00:00", "1008.0000", "-16.0000", false),
            ("2020-05-15 12:00:00", "0.0800", "7.9200", true),
        );

        let db = rates(&["2026-07-20"], &["2026-07-20"]);
        let sums = sums(events);
        assert_eq!(sums.gains_us_long(), usd("7.9200"));
        assert_eq!(sums.gains_us_short(), usd("-16.0000"));

        // The remaining BTC is the tail of the LT split (the ST split was fully sold).
        assert_pool(&state.exchange_balances.btc, &db, "0.99200000", "10.0000");
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
            ("2026-01-10 10:00:00", "1008.0000", "-16.0000", false),
            ("2020-05-15 12:00:00", "0.0800", "7.9200", true),
        );

        // Margin open fee: 0.02 BTC from the LT split. A nondeductible carrying cost:
        // a `Fee` atom (not `InvestmentFee`), proceeds go nowhere.
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
                reduces_proceeds,
            } => {
                assert!(!*reduces_proceeds);
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
                } => {
                    assert_eq!(asset_amount.to_decimal(), "-0.02000000".parse().unwrap());
                    assert_eq!(*proceeds, usd("20.0000"));
                    let us = long_us(net_gain);
                    assert_eq!(us.basis, usd("0.2000"));
                    assert_eq!(us.net_gain, usd("19.8000"));
                }
                other => panic!("expected investment fee atom, got {other:?}"),
            }
        }

        let db = rates(&["2026-07-20", "2026-07-25", "2026-07-26"], &["2026-07-20"]);
        let sums = sums(events);
        // LT trade gain: 7.92 (sale fee) + 19.80 (open fee) + 5 * 19.80 (rollover fees).
        assert_eq!(sums.us_long_trade_gain(), usd("126.7200"));
        // The rollover fee values ($100) are capped investment interest expense.
        assert_eq!(sums.us_long_position_fees(), usd("100.0000"));
        // 126.72 - 100 = 26.72, no carryover. ST is the sale's -$16 loss.
        assert_eq!(sums.gains_us_long(), usd("26.7200"));
        assert_eq!(sums.gains_us_short(), usd("-16.0000"));
        // Total proceeds: 1.00 BTC at the defined rate of $1,000.
        assert_eq!(sums.ledger_proceeds(), usd("1000.0000"));

        // Remaining BTC split: 0.872 at $10.
        assert_pool(&state.exchange_balances.btc, &db, "0.87200000", "10.0000");

        // ETH basis: 20 ETH at the defined rate of $50, i.e. $1,000.
        let eth: Vec<_> = state.exchange_balances.eth.iter().collect();
        assert_eq!(eth.len(), 1);
        let rate = eth[0]
            .lifecycle
            .get_exchange_rate_at_acquisition(&db)
            .unwrap();
        assert_eq!(rate, usd("50.0000"));
    }
}
