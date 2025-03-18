use crate::basis::{Asset, PoolAsset, PoolAssetSplit};
use crate::model::kraken_amount::KrakenAmount;
use crate::util::fifo::FIFO;
use std::collections::hash_map::{Drain, Entry, IntoIter, Iter};
use std::collections::HashMap;
use std::ops::{Add, Sub};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UtxoError {
    #[error("Invalid input TXID: {0}")]
    Input(String),

    #[error("A fee is required for every spend")]
    Fee,

    #[error("Error while splitting TXO basis")]
    SplitBasis(#[from] crate::basis::SplitBasisError),
}

/// Unspent Transaction Outputs.
///
/// This maps UTXOs in the form `txid:output_index` to a FIFO of _anything_. The
/// [`Utxo::transfer`] method is only available for [`PoolAsset`] items.
///
/// See [`UtxoBalances`] for more information.
///
/// [`UtxoBalances`]: crate::model::checkpoint::UtxoBalances
#[derive(Debug)]
pub struct Utxo<A> {
    utxos: HashMap<String, FIFO<A>>,
}

impl<A> Utxo<A> {
    pub(crate) fn new() -> Self {
        Self {
            utxos: HashMap::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.utxos.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.utxos.len()
    }

    pub(crate) fn iter(&self) -> Iter<'_, String, FIFO<A>> {
        self.utxos.iter()
    }

    pub(crate) fn drain(&mut self) -> Drain<'_, String, FIFO<A>> {
        self.utxos.drain()
    }

    pub fn entry<S>(&mut self, txid: S) -> Entry<'_, String, FIFO<A>>
    where
        S: ToString,
    {
        self.utxos.entry(txid.to_string())
    }

    pub(crate) fn remove<S>(&mut self, txid: S) -> Option<FIFO<A>>
    where
        S: AsRef<str>,
    {
        self.utxos.remove(txid.as_ref())
    }
}

impl<A: Asset> Utxo<PoolAsset<A>> {
    /// Transfer funds between our own wallets. Takes one or more TXO inputs and sends coins to one
    /// or more TXO outputs.
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
    ) -> Result<FIFO<PoolAsset<A>>, UtxoError>
    where
        A: Copy + Default + Add<Output = A> + Sub<Output = A>,
        <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
        PoolAsset<A>: PoolAssetSplit<Amount = A>,
        I: Iterator<Item = S>,
        O: IntoIterator<Item = (K, KrakenAmount, bool)>,
        S: AsRef<str>,
        K: ToString,
    {
        let mut fifo = self.inputs_into_fifo(inputs)?;
        self.insert_outputs(&mut fifo, outputs, None, true)?;

        // Require a non-empty FIFO to cover the transaction fee.
        if fifo.is_empty() {
            return Err(UtxoError::Fee);
        }

        Ok(fifo)
    }

    /// Spend funds from our wallet to unknown addresses. Takes one or more TXO inputs and sends
    /// coins to one or more TXO outputs.
    ///
    /// Returns one or more split assets used to cover the transaction fee, and all outputs as its
    /// own `Utxo`. This can be thought of as "subtracting" the inputs from `self` and returning the
    /// difference.
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
    ) -> Result<(FIFO<PoolAsset<A>>, Self), UtxoError>
    where
        A: Copy + Default + Add<Output = A> + Sub<Output = A>,
        <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
        PoolAsset<A>: PoolAssetSplit<Amount = A>,
        I: Iterator<Item = S>,
        O: IntoIterator<Item = (K, KrakenAmount, bool)>,
        S: AsRef<str>,
        K: ToString,
    {
        let mut output = Self::new();
        let mut fifo = self.inputs_into_fifo(inputs)?;
        self.insert_outputs(&mut fifo, outputs, Some(&mut output), require_fee)?;

        // Require a non-empty FIFO to cover the transaction fee.
        if require_fee && fifo.is_empty() {
            return Err(UtxoError::Fee);
        }

        Ok((fifo, output))
    }

    /// Receive funds in our wallet from an unknown address. Takes a list of transaction outputs
    /// (as received) and the cost basis FIFO to consume.
    pub(crate) fn receive<O, K>(
        &mut self,
        outputs: O,
        mut basis: FIFO<PoolAsset<A>>,
    ) -> Result<(), UtxoError>
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

            // Create a new UTXO for each output that belongs to us.
            self.utxos
                .insert(txid.to_string(), stw.takes.into_iter().collect());
        }

        Ok(())
    }

    fn inputs_into_fifo<I, S>(&mut self, inputs: I) -> Result<FIFO<PoolAsset<A>>, UtxoError>
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
    {
        // All inputs are consumed by the transfer. We combine all basis splits from all inputs into a
        // single FIFO to reduce complications from size disparities between inputs and outputs.
        // The only disparity we need to consider from here is whether the single input FIFO has
        // enough funds to cover the outputs and the transaction fee.
        let mut fifo = FIFO::new();
        for txid in inputs {
            let txid = txid.as_ref();
            match self.utxos.remove(txid) {
                Some(basis) => fifo.extend(basis),
                None => return Err(UtxoError::Input(txid.to_string())),
            }
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
    ) -> Result<(), UtxoError>
    where
        A: Copy + Default + Add<Output = A> + Sub<Output = A>,
        <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
        PoolAsset<A>: PoolAssetSplit<Amount = A>,
        O: IntoIterator<Item = (K, KrakenAmount, bool)>,
        K: ToString,
    {
        let mut has_outputs = false;
        for (txid, amount, mine) in outputs.into_iter() {
            // Each output is split from the large input FIFO. This handles both splitting and
            // combining as necessary to cover the transaction, while retaining "pure" basis origin
            // information.
            let mut stw = fifo.splittable_take_while(amount.try_into().unwrap())?;

            // Push the remainder to the front of the FIFO on each iteration.
            match (require_fee, stw.remain()) {
                (_, Some(remain)) => fifo.push_front(remain),
                (false, _) => (),
                (true, None) => return Err(UtxoError::Fee),
            };

            if mine {
                // Create a new UTXO for each output that belongs to us.
                self.utxos
                    .insert(txid.to_string(), stw.takes.into_iter().collect());
            } else if let Some(other) = other.as_mut() {
                // Create a new UTXO for each output that does not belong to us.
                other
                    .utxos
                    .insert(txid.to_string(), stw.takes.into_iter().collect());
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

impl<A> Default for Utxo<A> {
    fn default() -> Self {
        Self {
            utxos: HashMap::new(),
        }
    }
}

impl<A> FromIterator<(String, FIFO<A>)> for Utxo<A> {
    fn from_iter<T: IntoIterator<Item = (String, FIFO<A>)>>(iter: T) -> Self {
        let iterator = iter.into_iter();
        let mut utxo = Self::new();
        utxo.extend(iterator);

        utxo
    }
}

impl<A> IntoIterator for Utxo<A> {
    type Item = (String, FIFO<A>);
    type IntoIter = IntoIter<String, FIFO<A>>;

    fn into_iter(self) -> Self::IntoIter {
        self.utxos.into_iter()
    }
}

impl<A> Extend<(String, FIFO<A>)> for Utxo<A> {
    fn extend<T: IntoIterator<Item = (String, FIFO<A>)>>(&mut self, iter: T) {
        for (key, value) in iter.into_iter() {
            self.utxos.insert(key, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{basis::PoolBTC, model::ledgers::rows::BasisRow};
    use chrono::Utc;

    fn create_asset(amount: KrakenAmount) -> PoolBTC {
        PoolBTC::from_basis_row(&BasisRow {
            synthetic_id: ":0".to_string(),
            time: Utc::now(),
            asset: "XXBT".to_string(),
            amount: Some(amount),
            exchange_rate: "1.00".parse().unwrap(),
        })
    }

    #[test]
    #[should_panic]
    fn test_transfer_empty_inputs() {
        let amount = KrakenAmount::new("XXBT", "0.1").unwrap();
        let mut utxos = Utxo::from_iter([(
            "deadbeef:1".to_string(),
            FIFO::from_iter([create_asset(amount)]),
        )]);

        let _ = utxos.transfer::<_, _, &str, _>([].into_iter(), [("cafebabe:1", amount, true)]);
    }

    #[test]
    #[should_panic]
    fn test_transfer_empty_outputs() {
        let asset = create_asset(KrakenAmount::new("XXBT", "0.1").unwrap());
        let mut utxos = Utxo::from_iter([("deadbeef:1".to_string(), FIFO::from_iter([asset]))]);

        let _ = utxos.transfer::<_, _, _, &str>(["deadbeef:1"].into_iter(), None);
    }

    #[test]
    fn test_transfer_without_fee_simple() {
        let amount = KrakenAmount::new("XXBT", "0.1").unwrap();
        let mut utxos = Utxo::from_iter([(
            "deadbeef:1".to_string(),
            FIFO::from_iter([create_asset(amount)]),
        )]);

        let actual = utxos.transfer(["deadbeef:1"].into_iter(), [("cafebabe:1", amount, true)]);
        assert!(matches!(actual, Err(UtxoError::Fee)));
    }

    #[test]
    fn test_transfer_without_fee_complex() {
        let amount0 = KrakenAmount::new("XXBT", "0.1").unwrap();
        let amount1 = KrakenAmount::new("XXBT", "0.01").unwrap();
        let mut utxos = Utxo::from_iter([
            (
                "deadbeef:0".to_string(),
                FIFO::from_iter([create_asset(amount0)]),
            ),
            (
                "deadbeef:1".to_string(),
                FIFO::from_iter([create_asset(amount1)]),
            ),
        ]);

        let actual = utxos.transfer(
            ["deadbeef:0", "deadbeef:1"].into_iter(),
            [("cafebabe:1", amount0 + amount1, true)],
        );
        assert!(matches!(actual, Err(UtxoError::Fee)));
    }

    #[test]
    fn test_transfer_simple() -> Result<(), UtxoError> {
        let amount_in = KrakenAmount::new("XXBT", "0.11").unwrap();
        let mut utxos = Utxo::from_iter([(
            "deadbeef:0".to_string(),
            FIFO::from_iter([create_asset(amount_in)]),
        )]);

        let amount_out = KrakenAmount::new("XXBT", "0.1").unwrap();
        let actual = utxos.transfer(
            ["deadbeef:0"].into_iter(),
            [("cafebabe:0", amount_out, true)],
        )?;
        assert_eq!(actual.len(), 1);
        assert_eq!(
            actual[0].amount,
            KrakenAmount::new("XXBT", "0.01")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert!(utxos.utxos.contains_key("cafebabe:0"));

        Ok(())
    }

    #[test]
    fn test_transfer_complex() -> Result<(), UtxoError> {
        let amount_in_0 = KrakenAmount::new("XXBT", "0.1").unwrap();
        let amount_in_1 = KrakenAmount::new("XXBT", "0.05").unwrap();
        let amount_in_2 = KrakenAmount::new("XXBT", "0.025").unwrap();
        let amount_in_3 = KrakenAmount::new("XXBT", "0.02").unwrap();
        let amount_in_4 = KrakenAmount::new("XXBT", "0.005").unwrap();
        let mut utxos = Utxo::from_iter([
            (
                "deadbeef:0".to_string(),
                FIFO::from_iter([create_asset(amount_in_0), create_asset(amount_in_1)]),
            ),
            (
                "deadbeef:1".to_string(),
                FIFO::from_iter([create_asset(amount_in_2)]),
            ),
            (
                "deadbeef:2".to_string(),
                FIFO::from_iter([create_asset(amount_in_3), create_asset(amount_in_4)]),
            ),
        ]);

        let amount_out_0 = KrakenAmount::new("XXBT", "0.075").unwrap();
        let amount_out_1 = KrakenAmount::new("XXBT", "0.11").unwrap();
        let actual = utxos.transfer(
            ["deadbeef:0", "deadbeef:1", "deadbeef:2"].into_iter(),
            [
                ("cafebabe:0", amount_out_0, true),
                ("cafebabe:1", amount_out_1, false),
            ],
        )?;
        assert_eq!(actual.len(), 2);
        assert_eq!(
            actual[0].amount,
            KrakenAmount::new("XXBT", "0.01")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert_eq!(
            actual[1].amount,
            KrakenAmount::new("XXBT", "0.005")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert!(utxos.utxos.contains_key("cafebabe:0"));
        assert!(!utxos.utxos.contains_key("cafebabe:1"));

        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_spend_empty_inputs() {
        let amount = KrakenAmount::new("XXBT", "0.1").unwrap();
        let mut utxos = Utxo::from_iter([(
            "deadbeef:1".to_string(),
            FIFO::from_iter([create_asset(amount)]),
        )]);

        let _ = utxos.spend::<_, _, &str, _>([].into_iter(), [("cafebabe:1", amount, false)], true);
    }

    #[test]
    #[should_panic]
    fn test_spend_empty_outputs() {
        let asset = create_asset(KrakenAmount::new("XXBT", "0.1").unwrap());
        let mut utxos = Utxo::from_iter([("deadbeef:1".to_string(), FIFO::from_iter([asset]))]);

        let _ = utxos.spend::<_, _, _, &str>(["deadbeef:1"].into_iter(), None, true);
    }

    #[test]
    fn test_spend_without_fee_simple() {
        let amount = KrakenAmount::new("XXBT", "0.1").unwrap();
        let mut utxos = Utxo::from_iter([(
            "deadbeef:1".to_string(),
            FIFO::from_iter([create_asset(amount)]),
        )]);

        let actual = utxos.spend(
            ["deadbeef:1"].into_iter(),
            [("cafebabe:1", amount, false)],
            true,
        );
        assert!(matches!(actual, Err(UtxoError::Fee)));
    }

    #[test]
    fn test_spend_without_fee_complex() {
        let amount0 = KrakenAmount::new("XXBT", "0.1").unwrap();
        let amount1 = KrakenAmount::new("XXBT", "0.01").unwrap();
        let mut utxos = Utxo::from_iter([
            (
                "deadbeef:0".to_string(),
                FIFO::from_iter([create_asset(amount0)]),
            ),
            (
                "deadbeef:1".to_string(),
                FIFO::from_iter([create_asset(amount1)]),
            ),
        ]);

        let actual = utxos.spend(
            ["deadbeef:0", "deadbeef:1"].into_iter(),
            [("cafebabe:1", amount0 + amount1, false)],
            true,
        );
        assert!(matches!(actual, Err(UtxoError::Fee)));
    }

    #[test]
    fn test_spend_simple() -> Result<(), UtxoError> {
        let amount_in = KrakenAmount::new("XXBT", "0.11").unwrap();
        let mut utxos = Utxo::from_iter([(
            "deadbeef:0".to_string(),
            FIFO::from_iter([create_asset(amount_in)]),
        )]);

        let amount_out = KrakenAmount::new("XXBT", "0.1").unwrap();
        let (fee, spent) = utxos.spend(
            ["deadbeef:0"].into_iter(),
            [("cafebabe:0", amount_out, false)],
            true,
        )?;
        assert_eq!(fee.len(), 1);
        assert_eq!(
            fee[0].amount,
            KrakenAmount::new("XXBT", "0.01")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert!(!utxos.utxos.contains_key("cafebabe:0"));
        assert!(spent.utxos.contains_key("cafebabe:0"));

        Ok(())
    }

    #[test]
    fn test_spend_complex() -> Result<(), UtxoError> {
        let amount_in_0 = KrakenAmount::new("XXBT", "0.1").unwrap();
        let amount_in_1 = KrakenAmount::new("XXBT", "0.05").unwrap();
        let amount_in_2 = KrakenAmount::new("XXBT", "0.025").unwrap();
        let amount_in_3 = KrakenAmount::new("XXBT", "0.02").unwrap();
        let amount_in_4 = KrakenAmount::new("XXBT", "0.005").unwrap();
        let mut utxos = Utxo::from_iter([
            (
                "deadbeef:0".to_string(),
                FIFO::from_iter([create_asset(amount_in_0), create_asset(amount_in_1)]),
            ),
            (
                "deadbeef:1".to_string(),
                FIFO::from_iter([create_asset(amount_in_2)]),
            ),
            (
                "deadbeef:2".to_string(),
                FIFO::from_iter([create_asset(amount_in_3), create_asset(amount_in_4)]),
            ),
        ]);

        let amount_out_0 = KrakenAmount::new("XXBT", "0.075").unwrap();
        let amount_out_1 = KrakenAmount::new("XXBT", "0.11").unwrap();
        let (fee, spent) = utxos.spend(
            ["deadbeef:0", "deadbeef:1", "deadbeef:2"].into_iter(),
            [
                ("cafebabe:0", amount_out_0, true),
                ("cafebabe:1", amount_out_1, false),
            ],
            true,
        )?;
        assert_eq!(fee.len(), 2);
        assert_eq!(
            fee[0].amount,
            KrakenAmount::new("XXBT", "0.01")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert_eq!(
            fee[1].amount,
            KrakenAmount::new("XXBT", "0.005")
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert!(utxos.utxos.contains_key("cafebabe:0"));
        assert!(!utxos.utxos.contains_key("cafebabe:1"));
        assert!(!spent.utxos.contains_key("cafebabe:0"));
        assert!(spent.utxos.contains_key("cafebabe:1"));

        Ok(())
    }

    #[test]
    fn test_receive() -> Result<(), UtxoError> {
        let amount = KrakenAmount::new("XXBT", "0.11").unwrap();
        let mut utxos = Utxo::default();

        utxos.receive(
            [("deadbeef:0", amount)],
            FIFO::from_iter([create_asset(amount)]),
        )?;
        assert_eq!(
            utxos.utxos["deadbeef:0"].amount(),
            amount.try_into().unwrap(),
        );

        Ok(())
    }
}
