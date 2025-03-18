use crate::basis::AssetName;
use crate::model::{checkpoint, KrakenAmount};
use crate::model::{BitcoinAmount, EthWAmount, EtherAmount, FiatAmount, UsdcAmount, UsdtAmount};

#[derive(Debug, Default)]
pub struct Balances {
    pub btc: BitcoinAmount,
    pub chf: FiatAmount,
    pub eth: EtherAmount,
    pub ethw: EthWAmount,
    pub eur: FiatAmount,
    pub jpy: FiatAmount,
    pub usd: FiatAmount,
    pub usdc: UsdcAmount,
    pub usdt: UsdtAmount,
}

impl From<&checkpoint::Balances> for Balances {
    fn from(value: &checkpoint::Balances) -> Self {
        Self {
            btc: value.btc.amount(),
            chf: value.chf.amount().into(),
            eth: value.eth.amount(),
            ethw: value.ethw.amount(),
            eur: value.eur.amount().into(),
            jpy: value.jpy.amount().into(),
            usd: value.usd.amount().into(),
            usdc: value.usdc.amount(),
            usdt: value.usdt.amount(),
        }
    }
}

impl Balances {
    #[cfg(test)]
    pub(crate) fn get(&self, asset: AssetName) -> KrakenAmount {
        match asset {
            AssetName::Btc => KrakenAmount::from(self.btc),
            AssetName::Chf => KrakenAmount::try_from((asset, self.chf)).unwrap(),
            AssetName::Eth => KrakenAmount::from(self.eth),
            AssetName::EthW => KrakenAmount::from(self.ethw),
            AssetName::Eur => KrakenAmount::try_from((asset, self.eur)).unwrap(),
            AssetName::Jpy => KrakenAmount::try_from((asset, self.jpy)).unwrap(),
            AssetName::Usd => KrakenAmount::try_from((asset, self.usd)).unwrap(),
            AssetName::Usdc => KrakenAmount::from(self.usdc),
            AssetName::Usdt => KrakenAmount::from(self.usdt),
        }
    }

    pub fn accumulate(&mut self, amount: KrakenAmount, fee: KrakenAmount) {
        use KrakenAmount::*;

        match (amount, fee) {
            (Btc(amount), Btc(fee)) => {
                self.btc += amount - fee;
            }
            (Chf(amount), Chf(fee)) => {
                self.chf += amount - fee;
            }
            (Eur(amount), Eur(fee)) => {
                self.eur += amount - fee;
            }
            (Eth(amount), Eth(fee)) => {
                self.eth += amount - fee;
            }
            (EthW(amount), EthW(fee)) => {
                self.ethw += amount - fee;
            }
            (Jpy(amount), Jpy(fee)) => {
                self.jpy += amount - fee;
            }
            (Usd(amount), Usd(fee)) => {
                self.usd += amount - fee;
            }
            (Usdc(amount), Usdc(fee)) => {
                self.usdc += amount - fee;
            }
            (Usdt(amount), Usdt(fee)) => {
                self.usdt += amount - fee;
            }
            _ => unreachable!(),
        }
    }

    pub fn rebalance(
        &mut self,
        amount: KrakenAmount,
        fee: KrakenAmount,
        balance: &mut KrakenAmount,
    ) {
        use KrakenAmount::*;

        self.accumulate(amount, fee);

        match balance {
            Btc(balance) => {
                *balance = self.btc;
            }
            Chf(balance) => {
                *balance = self.chf;
            }
            Eur(balance) => {
                *balance = self.eur;
            }
            Eth(balance) => {
                *balance = self.eth;
            }
            EthW(balance) => {
                *balance = self.ethw;
            }
            Jpy(balance) => {
                *balance = self.jpy;
            }
            Usd(balance) => {
                *balance = self.usd;
            }
            Usdc(balance) => {
                *balance = self.usdc;
            }
            Usdt(balance) => {
                *balance = self.usdt;
            }
        }
    }

    pub fn eq<F, E>(&mut self, balance: KrakenAmount, map: F)
    where
        F: FnOnce(AssetName) -> E,
    {
        use KrakenAmount::*;

        let (asset, eq) = match balance {
            Btc(balance) => (AssetName::Btc, self.btc == balance),
            Chf(balance) => (AssetName::Chf, self.chf == balance),
            Eur(balance) => (AssetName::Eur, self.eur == balance),
            Eth(balance) => (AssetName::Eth, self.eth == balance),
            EthW(balance) => (AssetName::EthW, self.ethw == balance),
            Jpy(balance) => (AssetName::Jpy, self.jpy == balance),
            Usd(balance) => (AssetName::Usd, self.usd == balance),
            Usdc(balance) => (AssetName::Usdc, self.usdc == balance),
            Usdt(balance) => (AssetName::Usdt, self.usdt == balance),
        };

        if !eq {
            map(asset);
        }
    }
}
