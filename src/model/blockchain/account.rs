use crate::basis::{Asset, PoolAsset, PoolAssetSplit};
use crate::model::kraken_amount::KrakenAmount;
use crate::util::fifo::FIFO;
use std::collections::hash_map::{Drain, Entry, IntoIter, Iter};
use std::collections::HashMap;
use std::ops::{Add, Sub};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AccountError {
    #[error("Invalid input address: {0}")]
    Input(String),

    #[error("A fee is required for every spend")]
    Fee,

    #[error("Error while splitting TXO basis")]
    SplitBasis(#[from] crate::basis::SplitBasisError),
}

/// Accounting-based Blockchain model.
#[derive(Debug)]
pub struct Account<A> {
    addresses: HashMap<String, FIFO<A>>,
}

impl<A> Account<A> {
    pub(crate) fn new() -> Self {
        Self {
            addresses: HashMap::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.addresses.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.addresses.len()
    }

    pub(crate) fn iter(&self) -> Iter<'_, String, FIFO<A>> {
        self.addresses.iter()
    }

    pub(crate) fn drain(&mut self) -> Drain<'_, String, FIFO<A>> {
        self.addresses.drain()
    }

    pub fn entry<S>(&mut self, address: S) -> Entry<'_, String, FIFO<A>>
    where
        S: ToString,
    {
        self.addresses.entry(address.to_string())
    }

    pub(crate) fn remove<S>(&mut self, address: S) -> Option<FIFO<A>>
    where
        S: AsRef<str>,
    {
        self.addresses.remove(address.as_ref())
    }
}

impl<A: Asset> Account<PoolAsset<A>> {
    /// Transfer funds between our own wallets. Takes one or more address/amount tuple inputs and
    /// sends coins to one or more address/amount tuple outputs.
    ///
    /// Returns one or more split assets used to cover the transaction fee.
    ///
    /// In the event of an error, the state of `Self` is considered undefined.
    ///
    /// # Panics
    ///
    /// This method asserts that inputs and outputs are both non-empty.
    pub(crate) fn transfer<I, O, S, K>(
        &mut self,
        inputs: I,
        outputs: O,
    ) -> Result<FIFO<PoolAsset<A>>, AccountError>
    where
        A: Copy + Default + Add<Output = A> + Sub<Output = A>,
        <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
        PoolAsset<A>: PoolAssetSplit<Amount = A>,
        I: Iterator<Item = (S, KrakenAmount)>,
        O: IntoIterator<Item = (K, KrakenAmount, bool)>,
        S: AsRef<str>,
        K: ToString,
    {
        let mut fifo = self.inputs_into_fifo(inputs)?;
        self.insert_outputs(&mut fifo, outputs, None, true)?;

        // Require a non-empty FIFO to cover the transaction fee.
        if fifo.is_empty() {
            return Err(AccountError::Fee);
        }

        Ok(fifo)
    }

    /// Spend funds from our wallet to unknown addresses. Takes one or more address/amount tuple
    /// inputs and sends coins to one or more address/amount tuple outputs.
    ///
    /// Returns one or more split assets used to cover the transaction fee, and all outputs as its
    /// own `Account`. This can be thought of as "subtracting" the inputs from `self` and returning
    /// the difference.
    ///
    /// In the event of an error, the state of `Self` is considered undefined.
    ///
    /// # Panics
    ///
    /// This method asserts that inputs and outputs are both non-empty.
    pub(crate) fn spend<I, O, S, K>(
        &mut self,
        inputs: I,
        outputs: O,
        require_fee: bool,
    ) -> Result<(FIFO<PoolAsset<A>>, Self), AccountError>
    where
        A: Copy + Default + Add<Output = A> + Sub<Output = A>,
        <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
        PoolAsset<A>: PoolAssetSplit<Amount = A>,
        I: Iterator<Item = (S, KrakenAmount)>,
        O: IntoIterator<Item = (K, KrakenAmount, bool)>,
        S: AsRef<str>,
        K: ToString,
    {
        let mut output = Self::new();
        let mut fifo = self.inputs_into_fifo(inputs)?;
        self.insert_outputs(&mut fifo, outputs, Some(&mut output), require_fee)?;

        // Require a non-empty FIFO to cover the transaction fee.
        if require_fee && fifo.is_empty() {
            return Err(AccountError::Fee);
        }

        Ok((fifo, output))
    }

    /// Receive funds in our wallet from an unknown address. Takes a list of transaction outputs
    /// (as received) and the cost basis FIFO to consume.
    pub(crate) fn receive<O, K>(
        &mut self,
        outputs: O,
        mut basis: FIFO<PoolAsset<A>>,
    ) -> Result<(), AccountError>
    where
        A: Copy + Default + Add<Output = A> + Sub<Output = A>,
        <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
        PoolAsset<A>: PoolAssetSplit<Amount = A>,
        O: IntoIterator<Item = (K, KrakenAmount)>,
        K: ToString,
    {
        for (txid, amount) in outputs.into_iter() {
            let mut stw = basis.splittable_take_while(amount.try_into().unwrap())?;

            // Push the remainder to the front of the FIFO on each iteration.
            if let Some(remain) = stw.remain() {
                basis.push_front(remain)
            };

            // Create a new account address for each output that belongs to us.
            self.addresses
                .insert(txid.to_string(), stw.takes.into_iter().collect());
        }

        Ok(())
    }

    fn inputs_into_fifo<I, S>(&mut self, inputs: I) -> Result<FIFO<PoolAsset<A>>, AccountError>
    where
        A: Copy + Default + Add<Output = A> + Sub<Output = A>,
        <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
        PoolAsset<A>: PoolAssetSplit<Amount = A>,
        I: Iterator<Item = (S, KrakenAmount)>,
        S: AsRef<str>,
    {
        // Combine all basis splits from all inputs into a single FIFO to reduce complications from
        // size disparities between inputs and outputs. The only disparity we need to consider from
        // here is whether the single input FIFO has enough funds to cover the outputs and the
        // transaction fee.
        let mut fifo = FIFO::new();
        for (address, amount) in inputs {
            let address = address.as_ref();
            let taken_amount = match self.addresses.remove(address) {
                Some(mut fifo) => {
                    let mut stw = fifo.splittable_take_while(amount.try_into().unwrap())?;

                    // Push the remainder to the front of the FIFO on each iteration.
                    if let Some(remain) = stw.remain() {
                        fifo.push_front(remain);
                    }

                    // Reinsert the FIFO if it is not empty.
                    if !fifo.is_empty() {
                        self.addresses.insert(address.to_string(), fifo);
                    }

                    // Return the taken amount.
                    stw.takes.into_iter().collect::<Vec<_>>()
                }
                None => return Err(AccountError::Input(address.to_string())),
            };

            fifo.extend(taken_amount);
        }
        assert!(!fifo.is_empty());

        Ok(fifo)
    }

    fn insert_outputs<O, K>(
        &mut self,
        fifo: &mut FIFO<PoolAsset<A>>,
        outputs: O,
        mut other: Option<&mut Self>,
        require_fee: bool,
    ) -> Result<(), AccountError>
    where
        A: Copy + Default + Add<Output = A> + Sub<Output = A>,
        <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
        PoolAsset<A>: PoolAssetSplit<Amount = A>,
        O: IntoIterator<Item = (K, KrakenAmount, bool)>,
        K: ToString,
    {
        let mut has_outputs = false;
        for (address, amount, mine) in outputs.into_iter() {
            // Each output is split from the large input FIFO. This handles both splitting and
            // combining as necessary to cover the transaction, while retaining "pure" basis origin
            // information.
            let mut stw = fifo.splittable_take_while(amount.try_into().unwrap())?;

            // Push the remainder to the front of the FIFO on each iteration.
            match (require_fee, stw.remain()) {
                (_, Some(remain)) => fifo.push_front(remain),
                (false, _) => (),
                (true, None) => return Err(AccountError::Fee),
            };

            if mine {
                // Create a new account address for each output that belongs to us.
                self.addresses
                    .entry(address.to_string())
                    .and_modify(|entry| entry.extend(stw.takes.drain(..)))
                    .or_insert_with(|| stw.takes.drain(..).collect());
            } else if let Some(other) = other.as_mut() {
                // Create a new account address for each output that does not belong to us.
                other
                    .addresses
                    .insert(address.to_string(), stw.takes.into_iter().collect());
            }

            has_outputs = true;
        }

        // Require that one or more outputs was provided.
        // TODO: This is not necessary for the bitcoin protocol. E.g. the tx may only have a fee.
        // The assertion on `has_outputs` can be removed if fee-only transactions ever occur in
        // practice.
        assert!(has_outputs);

        Ok(())
    }
}

impl<A> Default for Account<A> {
    fn default() -> Self {
        Self {
            addresses: HashMap::new(),
        }
    }
}

impl<A> FromIterator<(String, FIFO<A>)> for Account<A> {
    fn from_iter<T: IntoIterator<Item = (String, FIFO<A>)>>(iter: T) -> Self {
        let iterator = iter.into_iter();
        let mut account = Self::new();
        account.extend(iterator);

        account
    }
}

impl<A> IntoIterator for Account<A> {
    type Item = (String, FIFO<A>);
    type IntoIter = IntoIter<String, FIFO<A>>;

    fn into_iter(self) -> Self::IntoIter {
        self.addresses.into_iter()
    }
}

impl<A> Extend<(String, FIFO<A>)> for Account<A> {
    fn extend<T: IntoIterator<Item = (String, FIFO<A>)>>(&mut self, iter: T) {
        for (key, value) in iter.into_iter() {
            self.addresses.insert(key, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{basis::PoolETH, model::ledgers::rows::BasisRow};
    use chrono::Utc;

    fn create_asset(amount: KrakenAmount) -> PoolETH {
        PoolETH::from_basis_row(&BasisRow {
            synthetic_id: ":0".to_string(),
            time: Utc::now(),
            asset: "XETH".to_string(),
            amount: Some(amount),
            exchange_rate: "1.00".parse().unwrap(),
        })
    }

    #[test]
    #[should_panic]
    fn test_transfer_empty_inputs() {
        let amount = KrakenAmount::new("XETH", "0.1").unwrap();
        let mut account = Account::from_iter([(
            "deadbeef".to_string(),
            FIFO::from_iter([create_asset(amount)]),
        )]);

        let _ = account.transfer::<_, _, &str, _>([].into_iter(), [("cafebabe", amount, true)]);
    }

    #[test]
    #[should_panic]
    fn test_transfer_empty_outputs() {
        let amount = KrakenAmount::new("XETH", "0.1").unwrap();
        let asset = create_asset(amount);
        let mut account = Account::from_iter([("deadbeef".to_string(), FIFO::from_iter([asset]))]);

        let _ = account.transfer::<_, _, _, &str>([("deadbeef", amount)].into_iter(), None);
    }

    #[test]
    fn test_transfer_without_fee_simple() {
        let amount = KrakenAmount::new("XETH", "0.1").unwrap();
        let mut account = Account::from_iter([(
            "deadbeef".to_string(),
            FIFO::from_iter([create_asset(amount)]),
        )]);

        let actual = account.transfer(
            [("deadbeef", amount)].into_iter(),
            [("cafebabe", amount, true)],
        );
        assert!(matches!(actual, Err(AccountError::Fee)));
    }

    #[test]
    fn test_transfer_without_fee_complex() {
        let amount0 = KrakenAmount::new("XETH", "0.1").unwrap();
        let amount1 = KrakenAmount::new("XETH", "0.01").unwrap();
        let mut account = Account::from_iter([
            (
                "deadbeef".to_string(),
                FIFO::from_iter([create_asset(amount0)]),
            ),
            (
                "baddc0de".to_string(),
                FIFO::from_iter([create_asset(amount1)]),
            ),
        ]);

        let actual = account.transfer(
            [("deadbeef", amount0), ("baddc0de", amount1)].into_iter(),
            [("cafebabe", amount0 + amount1, true)],
        );
        assert!(matches!(actual, Err(AccountError::Fee)));
    }

    #[test]
    fn test_transfer_simple() -> Result<(), AccountError> {
        let amount_in = KrakenAmount::new("XETH", "0.11").unwrap();
        let mut account = Account::from_iter([(
            "deadbeef".to_string(),
            FIFO::from_iter([create_asset(amount_in)]),
        )]);

        let amount_out = KrakenAmount::new("XETH", "0.1").unwrap();
        let actual = account.transfer(
            [("deadbeef", amount_in)].into_iter(),
            [("cafebabe", amount_out, true)],
        )?;
        assert_eq!(actual.len(), 1);
        assert_eq!(
            actual[0].amount,
            KrakenAmount::new("XETH", "0.01")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert!(account.addresses.contains_key("cafebabe"));

        Ok(())
    }

    #[test]
    fn test_transfer_complex() -> Result<(), AccountError> {
        let amount_0 = KrakenAmount::new("XETH", "0.1").unwrap();
        let amount_1 = KrakenAmount::new("XETH", "0.05").unwrap();
        let amount_2 = KrakenAmount::new("XETH", "0.025").unwrap();
        let amount_3 = KrakenAmount::new("XETH", "0.02").unwrap();
        let amount_4 = KrakenAmount::new("XETH", "0.005").unwrap();
        let mut account = Account::from_iter([
            (
                "deadbeef".to_string(),
                FIFO::from_iter([create_asset(amount_0), create_asset(amount_1)]),
            ),
            (
                "baddc0de".to_string(),
                FIFO::from_iter([create_asset(amount_2)]),
            ),
            (
                "c0ffee69".to_string(),
                FIFO::from_iter([create_asset(amount_3), create_asset(amount_4)]),
            ),
        ]);

        let amount_in_0 = KrakenAmount::new("XETH", "0.12").unwrap();
        let amount_in_1 = KrakenAmount::new("XETH", "0.025").unwrap();
        let amount_in_2 = KrakenAmount::new("XETH", "0.01").unwrap();
        let amount_out_0 = KrakenAmount::new("XETH", "0.03").unwrap();
        let amount_out_1 = KrakenAmount::new("XETH", "0.11").unwrap();
        let actual = account.transfer(
            [
                ("deadbeef", amount_in_0),
                ("baddc0de", amount_in_1),
                ("c0ffee69", amount_in_2),
            ]
            .into_iter(),
            [
                ("cafebabe", amount_out_0, true),
                ("feedface", amount_out_1, false),
            ],
        )?;
        assert_eq!(actual.len(), 2);
        assert_eq!(
            actual[0].amount,
            KrakenAmount::new("XETH", "0.005")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert_eq!(
            actual[1].amount,
            KrakenAmount::new("XETH", "0.01")
                .unwrap()
                .try_into()
                .unwrap(),
        );

        assert_eq!(
            account.addresses["deadbeef"][0].amount,
            KrakenAmount::new("XETH", "0.03")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert!(!account.addresses.contains_key("baddc0de"));
        assert_eq!(
            account.addresses["c0ffee69"][0].amount,
            KrakenAmount::new("XETH", "0.01")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert_eq!(
            account.addresses["c0ffee69"][1].amount,
            KrakenAmount::new("XETH", "0.005")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert!(account.addresses.contains_key("cafebabe"));
        assert!(!account.addresses.contains_key("feedface"));

        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_spend_empty_inputs() {
        let amount = KrakenAmount::new("XETH", "0.1").unwrap();
        let mut account = Account::from_iter([(
            "deadbeef".to_string(),
            FIFO::from_iter([create_asset(amount)]),
        )]);

        let _ = account.spend::<_, _, &str, _>([].into_iter(), [("cafebabe", amount, false)], true);
    }

    #[test]
    #[should_panic]
    fn test_spend_empty_outputs() {
        let amount = KrakenAmount::new("XETH", "0.1").unwrap();
        let asset = create_asset(amount);
        let mut account = Account::from_iter([("deadbeef".to_string(), FIFO::from_iter([asset]))]);

        let _ = account.spend::<_, _, _, &str>([("deadbeef", amount)].into_iter(), None, true);
    }

    #[test]
    fn test_spend_without_fee_simple() {
        let amount = KrakenAmount::new("XETH", "0.1").unwrap();
        let mut account = Account::from_iter([(
            "deadbeef".to_string(),
            FIFO::from_iter([create_asset(amount)]),
        )]);

        let actual = account.spend(
            [("deadbeef", amount)].into_iter(),
            [("cafebabe", amount, false)],
            true,
        );
        assert!(matches!(actual, Err(AccountError::Fee)));
    }

    #[test]
    fn test_spend_without_fee_complex() {
        let amount0 = KrakenAmount::new("XETH", "0.1").unwrap();
        let amount1 = KrakenAmount::new("XETH", "0.01").unwrap();
        let mut account = Account::from_iter([
            (
                "deadbeef".to_string(),
                FIFO::from_iter([create_asset(amount0)]),
            ),
            (
                "baddc0de".to_string(),
                FIFO::from_iter([create_asset(amount1)]),
            ),
        ]);

        let actual = account.spend(
            [("deadbeef", amount0), ("baddc0de", amount1)].into_iter(),
            [("cafebabe", amount0 + amount1, false)],
            true,
        );
        assert!(matches!(actual, Err(AccountError::Fee)));
    }

    #[test]
    fn test_spend_simple() -> Result<(), AccountError> {
        let amount_in = KrakenAmount::new("XETH", "0.11").unwrap();
        let mut account = Account::from_iter([(
            "deadbeef".to_string(),
            FIFO::from_iter([create_asset(amount_in)]),
        )]);

        let amount_out = KrakenAmount::new("XETH", "0.1").unwrap();
        let (fee, spent) = account.spend(
            [("deadbeef", amount_in)].into_iter(),
            [("cafebabe", amount_out, false)],
            true,
        )?;
        assert_eq!(fee.len(), 1);
        assert_eq!(
            fee[0].amount,
            KrakenAmount::new("XETH", "0.01")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert!(!account.addresses.contains_key("cafebabe"));
        assert!(spent.addresses.contains_key("cafebabe"));

        Ok(())
    }

    #[test]
    fn test_spend_complex() -> Result<(), AccountError> {
        let amount_0 = KrakenAmount::new("XETH", "0.1").unwrap();
        let amount_1 = KrakenAmount::new("XETH", "0.05").unwrap();
        let amount_2 = KrakenAmount::new("XETH", "0.025").unwrap();
        let amount_3 = KrakenAmount::new("XETH", "0.02").unwrap();
        let amount_4 = KrakenAmount::new("XETH", "0.005").unwrap();
        let mut account = Account::from_iter([
            (
                "deadbeef".to_string(),
                FIFO::from_iter([create_asset(amount_0), create_asset(amount_1)]),
            ),
            (
                "baddc0de".to_string(),
                FIFO::from_iter([create_asset(amount_2)]),
            ),
            (
                "c0ffee69".to_string(),
                FIFO::from_iter([create_asset(amount_3), create_asset(amount_4)]),
            ),
        ]);

        let amount_in_0 = KrakenAmount::new("XETH", "0.12").unwrap();
        let amount_in_1 = KrakenAmount::new("XETH", "0.025").unwrap();
        let amount_in_2 = KrakenAmount::new("XETH", "0.01").unwrap();
        let amount_out_0 = KrakenAmount::new("XETH", "0.03").unwrap();
        let amount_out_1 = KrakenAmount::new("XETH", "0.11").unwrap();
        let (fee, spent) = account.spend(
            [
                ("deadbeef", amount_in_0),
                ("baddc0de", amount_in_1),
                ("c0ffee69", amount_in_2),
            ]
            .into_iter(),
            [
                ("cafebabe", amount_out_0, true),
                ("feedface", amount_out_1, false),
            ],
            true,
        )?;
        assert_eq!(fee.len(), 2);
        assert_eq!(
            fee[0].amount,
            KrakenAmount::new("XETH", "0.005")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert_eq!(
            fee[1].amount,
            KrakenAmount::new("XETH", "0.01")
                .unwrap()
                .try_into()
                .unwrap(),
        );

        assert_eq!(
            account.addresses["deadbeef"][0].amount,
            KrakenAmount::new("XETH", "0.03")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert!(!account.addresses.contains_key("baddc0de"));
        assert_eq!(
            account.addresses["c0ffee69"][0].amount,
            KrakenAmount::new("XETH", "0.01")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert_eq!(
            account.addresses["c0ffee69"][1].amount,
            KrakenAmount::new("XETH", "0.005")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert!(account.addresses.contains_key("cafebabe"));
        assert!(!account.addresses.contains_key("feedface"));
        assert!(!spent.addresses.contains_key("cafebabe"));
        assert!(spent.addresses.contains_key("feedface"));

        Ok(())
    }

    #[test]
    fn test_receive() -> Result<(), AccountError> {
        let amount = KrakenAmount::new("XETH", "0.11").unwrap();
        let mut account = Account::default();

        account.receive(
            [("deadbeef", amount)],
            FIFO::from_iter([create_asset(amount)]),
        )?;
        assert_eq!(
            account.addresses["deadbeef"].amount(),
            amount.try_into().unwrap(),
        );

        Ok(())
    }
}
