use bdk::bitcoin::bip32::{ChildNumber, ExtendedPubKey};
use bdk::bitcoin::{base58, secp256k1, Network, Script, ScriptBuf};
use bdk::template::{Bip44Public, Bip49Public, Bip84Public, Bip86Public, DescriptorTemplate as _};
use bdk::KeychainKind;
use miniscript::descriptor::{DescriptorXKey, Wildcard};
use miniscript::{Descriptor, DescriptorPublicKey, ForEachKey as _};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};
use thiserror::Error;
use tracing::{debug, trace};

type KeyChainId = usize;
type ScriptMap = HashMap<ScriptBuf, KeyChainRef>;

/// `AddressCache` maintains a minimum number of "gap" addresses for `Auditor::is_mine()`. This
/// default value can be overridden with the `ADDRESS_GAP` environment variable. It cannot be set
/// to a value less than 25.
///
/// See [`AddressCache::is_mine`] for operation details.
static ADDRESS_GAP: LazyLock<u32> = LazyLock::new(|| {
    std::env::var("ADDRESS_GAP")
        .map(|value| {
            value
                .parse::<u32>()
                .expect("Unable to parse ADDRESS_GAP env var")
        })
        .unwrap_or_default()
        .max(25)
});

#[derive(Debug, Error)]
pub enum AddressError {
    #[error("BIP-32 error")]
    Bip32(#[from] bdk::bitcoin::bip32::Error),

    #[error("Miniscript error")]
    Miniscript(#[from] miniscript::Error),

    #[error("Address {0} does not belong to network {1}")]
    WrongNetwork(String, Network),

    #[error("The descriptor contains hardened derivation steps on public extended keys")]
    HardenedDerivationXpub,

    #[error("The descriptor contains multipath keys")]
    MultiPath,
}

/// An extended public key that remembers its original encoding.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Xpub {
    inner: ExtendedPubKey,
    encoded: String,
    version: XpubVersion,
}

/// Extended public key encoding versions.
///
/// Notably absent: "Ypub" and "Zpub".
#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
enum XpubVersion {
    /// "xpub" and "tpub"
    X,
    /// "ypub" and "upub"
    Y,
    /// "zpub" and "vpub"
    Z,
}

/// Address derivation scheme, aka HD Wallet Descriptor.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub(crate) enum AddressDerivation {
    /// P2PKH
    Bip44,
    /// P2WPKH-in-P2SH
    Bip49,
    /// P2WPKH
    Bip84,
    /// P2TR
    Bip86,
}

/// The top-level structure for caching addresses.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct AddressCache {
    /// Maps Script PubKeys ("addresses") to keychain references.
    scripts: ScriptMap,

    /// Each keychain derives addresses into the `ScriptMap`.
    keychains: Vec<KeyChain>,

    /// For lazy initialization. Each time the `AddressDerivation` scheme is _not found_ in this
    /// set, the `keychains` vector will be walked and all matching `KeyChain`s will be advanced to
    /// at least [`ADDRESS_GAP`].
    initialized: HashSet<AddressDerivation>,
}

/// A reference to index the `keychains` field and an index into the keychain.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct KeyChainRef {
    id: KeyChainId,
    address_index: u32,
}

/// A keychain that can derive addresses. Stores the maximum address index derived so far.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct KeyChain {
    xpub: Xpub,
    address_derivation: AddressDerivation,
    max_address_index: u32,
    descriptor: Descriptor<DescriptorPublicKey>,
    keychain_kind: KeychainKind,
}

impl Xpub {
    // `bitcoin` only supports parsing "xpub" version extended public keys.
    //
    // This handles:
    // - "xpub" BIP-44: P2PKH or P2WPKH-in-P2SH, BIP-84: P2WPKH, and BIP-86: P2TR
    // - "ypub" BIP-49: P2WPKH-in-P2SH
    // - "zpub" BIP-84: P2WPKH
    //
    // See: https://github.com/satoshilabs/slips/blob/master/slip-0132.md
    pub(crate) fn decode(xpub: &str, for_network: Network) -> Result<Self, AddressError> {
        let data = base58::decode_check(xpub).map_err(|err| AddressError::Bip32(err.into()))?;
        if data.len() != 78 {
            return Err(AddressError::Bip32(
                bdk::bitcoin::bip32::Error::WrongExtendedKeyLength(data.len()),
            ));
        }

        let (network, version) = match data[..4] {
            [0x04, 0x88, 0xb2, 0x1e] => (Network::Bitcoin, XpubVersion::X),
            [0x04, 0x9d, 0x7c, 0xb2] => (Network::Bitcoin, XpubVersion::Y),
            [0x04, 0xb2, 0x47, 0x46] => (Network::Bitcoin, XpubVersion::Z),

            [0x04, 0x35, 0x87, 0xcf] => (Network::Testnet, XpubVersion::X),
            [0x04, 0x4a, 0x52, 0x62] => (Network::Testnet, XpubVersion::Y),
            [0x04, 0x5f, 0x1c, 0xf6] => (Network::Testnet, XpubVersion::Z),

            _ => {
                return Err(AddressError::Bip32(
                    bdk::bitcoin::bip32::Error::UnknownVersion(data[..4].try_into().unwrap()),
                ));
            }
        };

        if network != for_network {
            return Err(AddressError::WrongNetwork(xpub.to_string(), for_network));
        }

        let inner = ExtendedPubKey {
            network,
            depth: data[4],
            parent_fingerprint: data[5..9].try_into().unwrap(),
            child_number: u32::from_be_bytes(data[9..13].try_into().unwrap()).into(),
            chain_code: data[13..45].try_into().unwrap(),
            public_key: secp256k1::PublicKey::from_slice(&data[45..78])
                .map_err(|err| AddressError::Bip32(err.into()))?,
        };

        Ok(Self {
            inner,
            encoded: xpub.to_string(),
            version,
        })
    }
}

impl TryFrom<&Script> for AddressDerivation {
    type Error = ();

    fn try_from(script_pubkey: &Script) -> Result<Self, ()> {
        if script_pubkey.is_p2pkh() {
            Ok(Self::Bip44)
        } else if script_pubkey.is_p2sh() {
            Ok(Self::Bip49)
        } else if script_pubkey.is_v0_p2wpkh() {
            Ok(Self::Bip84)
        } else if script_pubkey.is_v1_p2tr() {
            Ok(Self::Bip86)
        } else {
            Err(())
        }
    }
}

impl AddressCache {
    pub(crate) fn add_xpub(&mut self, xpub: Xpub) {
        // See: https://github.com/satoshilabs/slips/blob/master/slip-0132.md#motivation
        if xpub.version == XpubVersion::X {
            // "xpub" can derive addresses for all the BIPs.
            self.keychains.push(KeyChain::new(
                xpub.clone(),
                AddressDerivation::Bip44,
                KeychainKind::Internal,
            ));
            self.keychains.push(KeyChain::new(
                xpub.clone(),
                AddressDerivation::Bip44,
                KeychainKind::External,
            ));

            self.keychains.push(KeyChain::new(
                xpub.clone(),
                AddressDerivation::Bip49,
                KeychainKind::Internal,
            ));
            self.keychains.push(KeyChain::new(
                xpub.clone(),
                AddressDerivation::Bip49,
                KeychainKind::External,
            ));

            self.keychains.push(KeyChain::new(
                xpub.clone(),
                AddressDerivation::Bip84,
                KeychainKind::Internal,
            ));
            self.keychains.push(KeyChain::new(
                xpub.clone(),
                AddressDerivation::Bip84,
                KeychainKind::External,
            ));

            self.keychains.push(KeyChain::new(
                xpub.clone(),
                AddressDerivation::Bip86,
                KeychainKind::Internal,
            ));
            self.keychains.push(KeyChain::new(
                xpub,
                AddressDerivation::Bip86,
                KeychainKind::External,
            ));
        } else if xpub.version == XpubVersion::Y {
            // "ypub" can derive addresses for BIP-49.
            self.keychains.push(KeyChain::new(
                xpub.clone(),
                AddressDerivation::Bip49,
                KeychainKind::Internal,
            ));
            self.keychains.push(KeyChain::new(
                xpub,
                AddressDerivation::Bip49,
                KeychainKind::External,
            ));
        } else if xpub.version == XpubVersion::Z {
            // "zpub" can derive addresses for BIP-84.
            self.keychains.push(KeyChain::new(
                xpub.clone(),
                AddressDerivation::Bip84,
                KeychainKind::Internal,
            ));
            self.keychains.push(KeyChain::new(
                xpub,
                AddressDerivation::Bip84,
                KeychainKind::External,
            ));
        }
    }

    /// Check if this address cache contains the `xpub`.
    pub(crate) fn contains_xpub(&self, xpub: &str) -> bool {
        self.keychains
            .iter()
            .any(|keychain| keychain.xpub.encoded == xpub)
    }

    /// Check if the cache has been lazily initialized.
    pub(crate) fn initialized(&self) -> bool {
        !self.initialized.is_empty()
    }

    /// Initialize all keychains with the minimum address gap.
    ///
    /// If `predicate` is set, only matching keychains will be initialized. This is used for lazy
    /// address derivation.
    pub(crate) fn initialize(&mut self, predicate: Option<AddressDerivation>) {
        let min_address_gap = *ADDRESS_GAP;
        let script_maps = Arc::new(Mutex::new(Vec::with_capacity(self.keychains.len())));

        if let Some(address_derivation) = predicate {
            debug!("Lazily deriving {min_address_gap} addresses for {address_derivation:?}");
        } else {
            debug!("Deriving {min_address_gap} addresses for all keychains");
        }

        // Derive all addresses in parallel on the Rayon threadpool.
        rayon::scope(|scope| {
            for (id, keychain) in self.keychains.iter_mut().enumerate() {
                match predicate {
                    Some(ad) if keychain.address_derivation != ad => continue,
                    _ => (),
                }

                scope.spawn({
                    let script_maps = script_maps.clone();
                    move |_| {
                        let script_map = keychain.lookahead(id, 0);
                        if let Some(script_map) = script_map {
                            script_maps.lock().push(script_map);
                        }
                    }
                });
            }
        });

        let script_maps = Arc::into_inner(script_maps).unwrap().into_inner();
        for script_map in script_maps {
            self.merge(script_map);
        }

        if let Some(address_derivation) = predicate {
            self.initialized.insert(address_derivation);
        } else {
            self.initialized.insert(AddressDerivation::Bip44);
            self.initialized.insert(AddressDerivation::Bip49);
            self.initialized.insert(AddressDerivation::Bip84);
            self.initialized.insert(AddressDerivation::Bip86);
        }
    }

    /// Check if the `script_pubkey` (address) belongs to this cache.
    ///
    /// Derives new addresses on the matched keychain to maintain the lookahead.
    pub(crate) fn is_mine(&mut self, script_pubkey: &Script) -> bool {
        let Ok(address_derivation) = AddressDerivation::try_from(script_pubkey) else {
            return false;
        };

        // Lazily initialize all keychains for the script pub key's address derivation scheme.
        if !self.initialized.contains(&address_derivation) {
            self.initialize(Some(address_derivation));
        }

        if let Some(&KeyChainRef { id, address_index }) = self.scripts.get(script_pubkey) {
            // Maintain lookahead.
            if let Some(script_map) = self.keychains[id].lookahead(id, address_index) {
                self.merge(script_map);
            }

            true
        } else {
            false
        }
    }

    /// Merge the `ScriptMap` into this address cache.
    fn merge(&mut self, map: ScriptMap) {
        self.scripts.extend(map);
    }
}

impl KeyChain {
    /// Create a new address keychain.
    fn new(xpub: Xpub, address_derivation: AddressDerivation, keychain_kind: KeychainKind) -> Self {
        // Use bdk template to build a miniscript descriptor
        let fingerprint = xpub.inner.fingerprint();
        let (descriptor, _, _) = match address_derivation {
            AddressDerivation::Bip44 => {
                Bip44Public(xpub.inner, fingerprint, keychain_kind).build(xpub.inner.network)
            }
            AddressDerivation::Bip49 => {
                Bip49Public(xpub.inner, fingerprint, keychain_kind).build(xpub.inner.network)
            }
            AddressDerivation::Bip84 => {
                Bip84Public(xpub.inner, fingerprint, keychain_kind).build(xpub.inner.network)
            }
            AddressDerivation::Bip86 => {
                Bip86Public(xpub.inner, fingerprint, keychain_kind).build(xpub.inner.network)
            }
        }
        .expect("BDK template build failed");

        check_descriptor(&descriptor).expect("miniscript descriptor check failed");

        Self {
            xpub,
            address_derivation,
            max_address_index: 0,
            descriptor,
            keychain_kind,
        }
    }

    /// Derive new addresses to maintain the lookahead.
    fn lookahead(&mut self, id: KeyChainId, address_index: u32) -> Option<ScriptMap> {
        let max_address_index = address_index + *ADDRESS_GAP;
        if max_address_index > self.max_address_index {
            let new_addresses = max_address_index - self.max_address_index;

            trace!(
                "Deriving {new_addresses} addresses for `{}`, {:?}, {:?}",
                self.xpub.encoded,
                self.address_derivation,
                self.keychain_kind,
            );

            let mut script_map = ScriptMap::new();
            for address_index in self.max_address_index..max_address_index {
                let script_buf = self.derive(address_index);
                let keychain_ref = KeyChainRef { id, address_index };

                script_map.insert(script_buf, keychain_ref);
            }

            self.max_address_index = max_address_index;

            Some(script_map)
        } else {
            None
        }
    }

    fn derive(&self, address_index: u32) -> ScriptBuf {
        self.descriptor
            .at_derivation_index(address_index)
            .expect("Address derivation index overflow")
            .script_pubkey()
    }
}

// Copied from `bdk::descriptor::into_wallet_descriptor_checked()`
fn check_descriptor(descriptor: &Descriptor<DescriptorPublicKey>) -> Result<(), AddressError> {
    // Ensure the keys don't contain any hardened derivation steps or hardened wildcards
    let descriptor_contains_hardened_steps = descriptor.for_any_key(|k| {
        if let DescriptorPublicKey::XPub(DescriptorXKey {
            derivation_path,
            wildcard,
            ..
        }) = k
        {
            return *wildcard == Wildcard::Hardened
                || derivation_path.into_iter().any(ChildNumber::is_hardened);
        }

        false
    });
    if descriptor_contains_hardened_steps {
        return Err(AddressError::HardenedDerivationXpub);
    }

    if descriptor.is_multipath() {
        return Err(AddressError::MultiPath);
    }

    // Run miniscript's sanity check, which will look for duplicated keys and other potential
    // issues
    descriptor.sanity_check()?;

    Ok(())
}
