use crate::basis::AssetName;

// TODO: Kraken trading pairs are documented here:
// https://support.kraken.com/hc/en-us/articles/202944246-All-available-currencies-and-trading-pairs-on-Kraken
// https://support.kraken.com/hc/en-us/articles/227876608-Margin-trading-pairs-and-their-maximum-leverage
// https://support.kraken.com/hc/en-us/articles/kraken-markets

#[derive(Clone, Copy, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) enum Pair {
    BtcChf,
    BtcEur,
    BtcJpy,
    BtcUsd,
    BtcUsdc,
    BtcUsdt,

    EthBtc,
    EthChf,
    EthEur,
    EthJpy,
    EthUsd,
    EthUsdc,
    EthUsdt,

    EthWEth,
    EthWEur,
    EthWUsd,

    EurChf,
    EurJpy,
    EurUsd,

    UsdChf,
    UsdJpy,

    UsdcChf,
    UsdcEur,
    UsdcUsd,
    UsdcUsdt,

    UsdtChf,
    UsdtEur,
    UsdtJpy,
    UsdtUsd,
}

#[cfg(test)]
impl Pair {
    pub(crate) fn as_kraken(self) -> &'static str {
        use Pair::*;

        match self {
            BtcChf => "XBTCHF",
            BtcEur => "XXBTZEUR",
            BtcJpy => "XXBTZJPY",
            BtcUsd => "XXBTZUSD",
            BtcUsdc => "XBTUSDC",
            BtcUsdt => "XBTUSDT",

            EthBtc => "XETHXXBT",
            EthChf => "XETHCHF",
            EthEur => "XETHEUR",
            EthJpy => "XETHJPY",
            EthUsd => "XETHZUSD",
            EthUsdc => "XETHUSDC",
            EthUsdt => "XETHUSDT",

            EthWEth => "ETHWXETH",
            EthWEur => "ETHWEUR",
            EthWUsd => "ETHWZUSD",

            EurChf => "EURCHF",
            EurJpy => "EURJPY",
            EurUsd => "EURZUSD",

            UsdChf => "ZUSDCHF",
            UsdJpy => "ZUSDJPY", // Not valid for margin

            UsdcChf => "USDCCHF",
            UsdcEur => "USDCEUR",
            UsdcUsd => "USDCZUSD",
            UsdcUsdt => "USDCUSDT",

            UsdtChf => "USDTCHF",
            UsdtEur => "USDTEUR",
            UsdtJpy => "USDTJPY",
            UsdtUsd => "USDTZUSD",
        }
    }

    pub(crate) fn get_base(self) -> AssetName {
        use Pair::*;

        match self {
            BtcChf | BtcEur | BtcJpy | BtcUsd | BtcUsdc | BtcUsdt => AssetName::Btc,
            EthBtc | EthChf | EthEur | EthJpy | EthUsd | EthUsdc | EthUsdt => AssetName::Eth,
            EthWEth | EthWEur | EthWUsd => AssetName::EthW,
            EurChf | EurJpy | EurUsd => AssetName::Eur,
            UsdChf | UsdJpy => AssetName::Usd,
            UsdcChf | UsdcEur | UsdcUsd | UsdcUsdt => AssetName::Usdc,
            UsdtChf | UsdtEur | UsdtJpy | UsdtUsd => AssetName::Usdt,
        }
    }

    pub(crate) fn get_quote(self) -> AssetName {
        use Pair::*;

        match self {
            EthBtc => AssetName::Btc,
            BtcChf | EthChf | EurChf | UsdChf | UsdcChf | UsdtChf => AssetName::Chf,
            EthWEth => AssetName::Eth,
            BtcEur | EthEur | EthWEur | UsdcEur | UsdtEur => AssetName::Eur,
            BtcJpy | EthJpy | EurJpy | UsdJpy | UsdtJpy => AssetName::Jpy,
            BtcUsd | EthUsd | EthWUsd | EurUsd | UsdtUsd | UsdcUsd => AssetName::Usd,
            BtcUsdc | EthUsdc => AssetName::Usdc,
            BtcUsdt | EthUsdt | UsdcUsdt => AssetName::Usdt,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Trade {
    Buy,
    Sell,
}

pub(crate) fn get_asset_pair(first: AssetName, second: AssetName) -> (Pair, Trade) {
    match (first, second) {
        (AssetName::Btc, AssetName::Chf) => (Pair::BtcChf, Trade::Sell),
        (AssetName::Chf, AssetName::Btc) => (Pair::BtcChf, Trade::Buy),

        (AssetName::Btc, AssetName::Eur) => (Pair::BtcEur, Trade::Sell),
        (AssetName::Eur, AssetName::Btc) => (Pair::BtcEur, Trade::Buy),

        (AssetName::Btc, AssetName::Jpy) => (Pair::BtcJpy, Trade::Sell),
        (AssetName::Jpy, AssetName::Btc) => (Pair::BtcJpy, Trade::Buy),

        (AssetName::Btc, AssetName::Usd) => (Pair::BtcUsd, Trade::Sell),
        (AssetName::Usd, AssetName::Btc) => (Pair::BtcUsd, Trade::Buy),

        (AssetName::Btc, AssetName::Usdc) => (Pair::BtcUsdc, Trade::Sell),
        (AssetName::Usdc, AssetName::Btc) => (Pair::BtcUsdc, Trade::Buy),

        (AssetName::Btc, AssetName::Usdt) => (Pair::BtcUsdt, Trade::Sell),
        (AssetName::Usdt, AssetName::Btc) => (Pair::BtcUsdt, Trade::Buy),

        (AssetName::Eth, AssetName::Btc) => (Pair::EthBtc, Trade::Sell),
        (AssetName::Btc, AssetName::Eth) => (Pair::EthBtc, Trade::Buy),

        (AssetName::Eth, AssetName::Chf) => (Pair::EthChf, Trade::Sell),
        (AssetName::Chf, AssetName::Eth) => (Pair::EthChf, Trade::Buy),

        (AssetName::Eth, AssetName::Eur) => (Pair::EthEur, Trade::Sell),
        (AssetName::Eur, AssetName::Eth) => (Pair::EthEur, Trade::Buy),

        (AssetName::Eth, AssetName::Jpy) => (Pair::EthJpy, Trade::Sell),
        (AssetName::Jpy, AssetName::Eth) => (Pair::EthJpy, Trade::Buy),

        (AssetName::Eth, AssetName::Usd) => (Pair::EthUsd, Trade::Sell),
        (AssetName::Usd, AssetName::Eth) => (Pair::EthUsd, Trade::Buy),

        (AssetName::Eth, AssetName::Usdc) => (Pair::EthUsdc, Trade::Sell),
        (AssetName::Usdc, AssetName::Eth) => (Pair::EthUsdc, Trade::Buy),

        (AssetName::Eth, AssetName::Usdt) => (Pair::EthUsdt, Trade::Sell),
        (AssetName::Usdt, AssetName::Eth) => (Pair::EthUsdt, Trade::Buy),

        (AssetName::EthW, AssetName::Eth) => (Pair::EthWEth, Trade::Sell),
        (AssetName::Eth, AssetName::EthW) => (Pair::EthWEth, Trade::Buy),

        (AssetName::EthW, AssetName::Eur) => (Pair::EthWEur, Trade::Sell),
        (AssetName::Eur, AssetName::EthW) => (Pair::EthWEur, Trade::Buy),

        (AssetName::EthW, AssetName::Usd) => (Pair::EthWUsd, Trade::Sell),
        (AssetName::Usd, AssetName::EthW) => (Pair::EthWUsd, Trade::Buy),

        (AssetName::Eur, AssetName::Chf) => (Pair::EurChf, Trade::Sell),
        (AssetName::Chf, AssetName::Eur) => (Pair::EurChf, Trade::Buy),

        (AssetName::Eur, AssetName::Jpy) => (Pair::EurJpy, Trade::Sell),
        (AssetName::Jpy, AssetName::Eur) => (Pair::EurJpy, Trade::Buy),

        (AssetName::Eur, AssetName::Usd) => (Pair::EurUsd, Trade::Sell),
        (AssetName::Usd, AssetName::Eur) => (Pair::EurUsd, Trade::Buy),

        (AssetName::Usd, AssetName::Chf) => (Pair::UsdChf, Trade::Sell),
        (AssetName::Chf, AssetName::Usd) => (Pair::UsdChf, Trade::Buy),

        (AssetName::Usd, AssetName::Jpy) => (Pair::UsdJpy, Trade::Sell),
        (AssetName::Jpy, AssetName::Usd) => (Pair::UsdJpy, Trade::Buy),

        (AssetName::Usdc, AssetName::Chf) => (Pair::UsdcChf, Trade::Sell),
        (AssetName::Chf, AssetName::Usdc) => (Pair::UsdcChf, Trade::Buy),

        (AssetName::Usdc, AssetName::Eur) => (Pair::UsdcEur, Trade::Sell),
        (AssetName::Eur, AssetName::Usdc) => (Pair::UsdcEur, Trade::Buy),

        (AssetName::Usdc, AssetName::Usd) => (Pair::UsdcUsd, Trade::Sell),
        (AssetName::Usd, AssetName::Usdc) => (Pair::UsdcUsd, Trade::Buy),

        (AssetName::Usdc, AssetName::Usdt) => (Pair::UsdcUsdt, Trade::Sell),
        (AssetName::Usdt, AssetName::Usdc) => (Pair::UsdcUsdt, Trade::Buy),

        (AssetName::Usdt, AssetName::Chf) => (Pair::UsdtChf, Trade::Sell),
        (AssetName::Chf, AssetName::Usdt) => (Pair::UsdtChf, Trade::Buy),

        (AssetName::Usdt, AssetName::Usd) => (Pair::UsdtUsd, Trade::Sell),
        (AssetName::Usd, AssetName::Usdt) => (Pair::UsdtUsd, Trade::Buy),

        (AssetName::Usdt, AssetName::Eur) => (Pair::UsdtEur, Trade::Sell),
        (AssetName::Eur, AssetName::Usdt) => (Pair::UsdtEur, Trade::Buy),

        (AssetName::Usdt, AssetName::Jpy) => (Pair::UsdtJpy, Trade::Sell),
        (AssetName::Jpy, AssetName::Usdt) => (Pair::UsdtJpy, Trade::Buy),

        // Invalid pairs
        (AssetName::Btc, AssetName::Btc)
        | (AssetName::Chf, AssetName::Chf)
        | (AssetName::Eth, AssetName::Eth)
        | (AssetName::EthW, AssetName::EthW)
        | (AssetName::Eur, AssetName::Eur)
        | (AssetName::Jpy, AssetName::Jpy)
        | (AssetName::Usd, AssetName::Usd)
        | (AssetName::Usdc, AssetName::Usdc)
        | (AssetName::Usdt, AssetName::Usdt)
        | (AssetName::EthW, AssetName::Btc)
        | (AssetName::Btc, AssetName::EthW)
        | (AssetName::EthW, AssetName::Chf)
        | (AssetName::Chf, AssetName::EthW)
        | (AssetName::EthW, AssetName::Jpy)
        | (AssetName::Jpy, AssetName::EthW)
        | (AssetName::EthW, AssetName::Usdc)
        | (AssetName::Usdc, AssetName::EthW)
        | (AssetName::EthW, AssetName::Usdt)
        | (AssetName::Usdt, AssetName::EthW)
        | (AssetName::Jpy, AssetName::Chf)
        | (AssetName::Chf, AssetName::Jpy)
        | (AssetName::Usdc, AssetName::Jpy)
        | (AssetName::Jpy, AssetName::Usdc) => unreachable!(),
    }
}
