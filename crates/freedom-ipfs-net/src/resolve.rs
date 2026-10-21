//! The gate's resolver (design 0007, "The resolver contract").
//!
//! It sits under every ordinary name of a client that checks addresses, as
//! reqwest's `dns_resolver`. It answers one of two things:
//! - one or more addresses, every one of which passed the gate (for an
//!   untrusted client; a configured target's client takes them as they
//!   come);
//! - an error.
//!
//! It never answers an empty success: a failed lookup, an empty answer and
//! an answer whose every address the policy refuses are all errors. Onion
//! names never reach it (the hop loop refuses them first), and it answers
//! one with an error, without a lookup, in case one ever arrives.
//!
//! Lookups are async and cancellable: hickory's, over the system's DNS
//! configuration or Cloudflare's resolvers where that can't be read, never
//! `getaddrinfo` on a blocking thread, so an instance's teardown never waits
//! for a lookup nothing can cancel.

use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use hickory_resolver::config::ResolverConfig;
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::TokioResolver;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

use crate::capability::{NetworkCapability, Revoked};
use crate::gate::{check_address, classify_host, AddressRefusal, Nat64State, Target};
use crate::policy::Nat64Handle;

/// A boxed lookup in flight.
pub type LookupFuture = Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, LookupError>> + Send>>;

/// What the gate's resolver asks for a name's addresses.
pub trait NameLookup: fmt::Debug + Send + Sync {
    /// The addresses of `name`, an ASCII name the gate has classified as an
    /// ordinary one: lowercase, with no trailing dot.
    fn lookup(&self, name: &str) -> LookupFuture;
}

/// A lookup that failed.
#[derive(Debug, thiserror::Error)]
#[error("the name couldn't be resolved: {0}")]
pub struct LookupError(pub String);

/// hickory's resolver, over the system's DNS configuration, or Cloudflare's
/// resolvers if it can't be read.
pub(crate) fn system_lookup() -> Arc<dyn NameLookup> {
    let resolver = match TokioResolver::builder_tokio() {
        Ok(builder) => builder.build(),
        Err(_) => TokioResolver::builder_with_config(
            ResolverConfig::cloudflare(),
            TokioConnectionProvider::default(),
        )
        .build(),
    };
    Arc::new(HickoryLookup(resolver))
}

struct HickoryLookup(TokioResolver);

impl fmt::Debug for HickoryLookup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HickoryLookup")
    }
}

impl NameLookup for HickoryLookup {
    fn lookup(&self, name: &str) -> LookupFuture {
        let resolver = self.0.clone();
        // A fully qualified name: the system's search domains don't apply.
        let name = format!("{name}.");
        Box::pin(async move {
            resolver
                .lookup_ip(name)
                .await
                .map(|lookup| lookup.iter().collect())
                .map_err(|err| LookupError(err.kind().to_string()))
        })
    }
}

/// Why the gate's resolver answered with an error. Each text names the
/// rule, never the name or an address.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ResolveRefused {
    /// The capability is revoked: before the lookup, while it or the wait
    /// for NAT64 discovery ran, or as the answer settled. The [`Revoked`]
    /// is its source, so reqwest's error still reads as revoked.
    #[error("{0}")]
    Revoked(#[from] Revoked),
    #[error("the name isn't one the gate accepts: {0}")]
    Target(crate::gate::Refusal),
    #[error("an onion name is never resolved here")]
    Onion,
    #[error(transparent)]
    Lookup(#[from] LookupError),
    #[error("the name has no address")]
    Empty,
    #[error("the name has no address the policy accepts; the first was refused: {0}")]
    AllRefused(AddressRefusal),
}

/// The gate's resolver, as reqwest's `dns_resolver`.
#[derive(Clone, Debug)]
pub(crate) struct GateResolver {
    lookup: Arc<dyn NameLookup>,
    capability: NetworkCapability,
    /// The address policy, for an untrusted client's resolver; `None` for a
    /// configured target's, which takes every address as it comes.
    addresses: Option<Nat64Handle>,
}

impl GateResolver {
    pub(crate) fn new(
        lookup: Arc<dyn NameLookup>,
        capability: NetworkCapability,
        addresses: Option<Nat64Handle>,
    ) -> Self {
        Self {
            lookup,
            capability,
            addresses,
        }
    }

    /// The addresses `host` may be connected to: at least one, or an error.
    ///
    /// The lookup and any wait for NAT64 discovery run under a permit of
    /// the capability: revoked meanwhile, they are dropped and this fails
    /// with [`ResolveRefused::Revoked`]. It is asked once more just before
    /// the addresses are returned.
    pub(crate) async fn resolve_host(&self, host: &str) -> Result<Vec<IpAddr>, ResolveRefused> {
        let permit = self.capability.admit()?;
        let addrs = permit.run(self.resolve_admitted(host)).await??;
        self.capability.check()?;
        Ok(addrs)
    }

    async fn resolve_admitted(&self, host: &str) -> Result<Vec<IpAddr>, ResolveRefused> {
        let answers = match classify_host(host).map_err(ResolveRefused::Target)? {
            Target::Onion(_) => return Err(ResolveRefused::Onion),
            Target::Ip(ip) => vec![ip],
            Target::Name(name) => self.lookup.lookup(&name).await?,
        };
        if answers.is_empty() {
            return Err(ResolveRefused::Empty);
        }
        let Some(handle) = &self.addresses else {
            return Ok(answers);
        };
        // Every answer is judged against one settled state: an answer
        // accepted before discovery changed it might be refused after.
        let state = settled(&answers, handle.clone()).await;
        let mut accepted = Vec::new();
        let mut first_refusal = None;
        for ip in answers {
            match check_address(ip, &state) {
                Ok(connect) => {
                    if !accepted.contains(&connect) {
                        accepted.push(connect);
                    }
                }
                Err(refusal) => {
                    first_refusal.get_or_insert(refusal);
                }
            }
        }
        match (accepted.is_empty(), first_refusal) {
            (false, _) => Ok(accepted),
            (true, Some(refusal)) => Err(ResolveRefused::AllRefused(refusal)),
            (true, None) => Err(ResolveRefused::Empty),
        }
    }
}

/// The NAT64 state to judge `answers` by: the current one once none of them
/// waits for discovery, or once nothing will publish again. The caller's
/// deadline (reqwest's connect timeout, the operation's deadline) bounds
/// the wait.
pub(crate) async fn settled(answers: &[IpAddr], mut handle: Nat64Handle) -> Nat64State {
    loop {
        let state = handle.current();
        let waits = answers.iter().any(|ip| {
            matches!(
                check_address(*ip, &state),
                Err(AddressRefusal::Nat64Pending)
            )
        });
        if !waits || !handle.changed().await {
            return state;
        }
    }
}

/// The address policy for `ip`, once NAT64 discovery has settled for it
/// (see [`settled`]).
pub(crate) async fn check_waiting(
    ip: IpAddr,
    handle: &mut Nat64Handle,
) -> Result<IpAddr, AddressRefusal> {
    check_address(ip, &settled(&[ip], handle.clone()).await)
}

impl Resolve for GateResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let resolver = self.clone();
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs = resolver
                .resolve_host(&host)
                .await
                .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)?;
            let addrs: Addrs = Box::new(addrs.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok(addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::{Nat64Prefix, Nat64State};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// A name and what a lookup of it answers.
    type Answer = (String, Result<Vec<IpAddr>, String>);

    /// A lookup that answers from a table, counting its calls.
    #[derive(Debug, Default)]
    struct Table {
        answers: Mutex<Vec<Answer>>,
        calls: AtomicUsize,
    }

    impl Table {
        fn with(answers: &[(&str, Result<Vec<&str>, &str>)]) -> Arc<Self> {
            Arc::new(Self {
                answers: Mutex::new(
                    answers
                        .iter()
                        .map(|(name, answer)| {
                            (
                                name.to_string(),
                                answer
                                    .clone()
                                    .map(|ips| ips.iter().map(|ip| ip.parse().unwrap()).collect())
                                    .map_err(str::to_string),
                            )
                        })
                        .collect(),
                ),
                calls: AtomicUsize::new(0),
            })
        }
    }

    impl NameLookup for Table {
        fn lookup(&self, name: &str) -> LookupFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let answer = self
                .answers
                .lock()
                .unwrap()
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, answer)| answer.clone())
                .unwrap_or(Err("NXDOMAIN".into()));
            Box::pin(async move { answer.map_err(LookupError) })
        }
    }

    fn ips(list: &[&str]) -> Vec<IpAddr> {
        list.iter().map(|ip| ip.parse().unwrap()).collect()
    }

    fn untrusted(table: &Arc<Table>, state: Nat64State) -> GateResolver {
        GateResolver::new(
            table.clone(),
            NetworkCapability::new(),
            Some(Nat64Handle::fixed(state)),
        )
    }

    #[tokio::test]
    async fn an_untrusted_name_gets_only_the_addresses_the_gate_accepts() {
        let table = Table::with(&[
            (
                "mixed.example",
                Ok(vec!["10.0.0.1", "8.8.8.8", "::ffff:8.8.4.4"]),
            ),
            ("private.example", Ok(vec!["127.0.0.1", "169.254.169.254"])),
            ("dns64.example", Ok(vec!["64:ff9b::7f00:1"])),
        ]);
        let resolver = untrusted(&table, Nat64State::NONE);
        // A mapped answer is connected to as IPv4.
        assert_eq!(
            resolver.resolve_host("Mixed.Example.").await.unwrap(),
            ips(&["8.8.8.8", "8.8.4.4"])
        );
        for name in ["private.example", "dns64.example"] {
            assert!(matches!(
                resolver.resolve_host(name).await,
                Err(ResolveRefused::AllRefused(_))
            ));
        }
    }

    #[tokio::test]
    async fn it_never_answers_an_empty_success() {
        let table = Table::with(&[
            ("empty.example", Ok(vec![])),
            ("fail.example", Err("SERVFAIL")),
        ]);
        for resolver in [
            untrusted(&table, Nat64State::NONE),
            GateResolver::new(table.clone(), NetworkCapability::new(), None),
        ] {
            assert!(matches!(
                resolver.resolve_host("empty.example").await,
                Err(ResolveRefused::Empty)
            ));
            assert!(matches!(
                resolver.resolve_host("fail.example").await,
                Err(ResolveRefused::Lookup(_))
            ));
            assert!(matches!(
                resolver.resolve_host("unknown.example").await,
                Err(ResolveRefused::Lookup(_))
            ));
        }
    }

    #[tokio::test]
    async fn onions_literals_and_refusals_need_no_lookup() {
        let table = Table::with(&[]);
        let resolver = untrusted(&table, Nat64State::NONE);
        assert!(matches!(
            resolver.resolve_host("abc.onion").await,
            Err(ResolveRefused::Onion)
        ));
        assert!(matches!(
            resolver.resolve_host("bücher.example").await,
            Err(ResolveRefused::Target(_))
        ));
        // A literal, as reqwest passes one under a proxy: checked, not looked
        // up.
        assert!(matches!(
            resolver.resolve_host("[::1]").await,
            Err(ResolveRefused::AllRefused(_))
        ));
        assert_eq!(
            resolver.resolve_host("8.8.8.8").await.unwrap(),
            ips(&["8.8.8.8"])
        );
        assert_eq!(table.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_configured_targets_resolver_takes_its_answers_as_they_come() {
        let table = Table::with(&[("router.lan", Ok(vec!["192.168.1.5"]))]);
        let configured = GateResolver::new(table.clone(), NetworkCapability::new(), None);
        assert_eq!(
            configured.resolve_host("router.lan").await.unwrap(),
            ips(&["192.168.1.5"])
        );
        assert!(matches!(
            untrusted(&table, Nat64State::NONE)
                .resolve_host("router.lan")
                .await,
            Err(ResolveRefused::AllRefused(_))
        ));
    }

    #[tokio::test]
    async fn a_revoked_capability_resolves_nothing() {
        let table = Table::with(&[("example.com", Ok(vec!["8.8.8.8"]))]);
        let capability = NetworkCapability::new();
        capability.revoke();
        let resolver = GateResolver::new(table.clone(), capability, None);
        assert!(matches!(
            resolver.resolve_host("example.com").await,
            Err(ResolveRefused::Revoked(_))
        ));
        assert_eq!(table.calls.load(Ordering::SeqCst), 0);
    }

    /// Revoked while an answer waits for NAT64 discovery: discovery then
    /// accepts it, and still nothing is returned (#46 review, 46-M1).
    #[tokio::test]
    async fn revoked_during_a_nat64_wait_returns_no_address() {
        let table = Table::with(&[("public.example", Ok(vec!["2a00:1::1"]))]);
        let capability = NetworkCapability::new();
        let (publisher, handle) = Nat64Handle::channel(Nat64State::Pending);
        let resolver = GateResolver::new(table, capability.clone(), Some(handle));
        let resolving = tokio::spawn(async move { resolver.resolve_host("public.example").await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!resolving.is_finished(), "the answer waits for discovery");
        assert_eq!(capability.permits(), 1);
        capability.revoke();
        // The wait is dropped at the revoke, not when discovery answers.
        tokio::time::timeout(std::time::Duration::from_secs(1), capability.quiesced())
            .await
            .expect("the wait is dropped at the revoke");
        publisher.publish(Nat64State::NONE);
        let result = resolving.await.unwrap();
        assert!(
            matches!(result, Err(ResolveRefused::Revoked(_))),
            "{result:?}"
        );
        assert_eq!(capability.permits(), 0);
    }

    /// Revoked after the answer settled, as it is returned: nothing is
    /// returned either.
    #[tokio::test]
    async fn revoked_as_the_answer_settles_returns_no_address() {
        /// A lookup that revokes the capability as it answers.
        #[derive(Debug)]
        struct RevokingLookup(NetworkCapability);

        impl NameLookup for RevokingLookup {
            fn lookup(&self, _: &str) -> LookupFuture {
                self.0.revoke();
                Box::pin(async { Ok(vec!["8.8.8.8".parse().unwrap()]) })
            }
        }

        let capability = NetworkCapability::new();
        let resolver = GateResolver::new(
            Arc::new(RevokingLookup(capability.clone())),
            capability,
            None,
        );
        assert!(matches!(
            resolver.resolve_host("example.com").await,
            Err(ResolveRefused::Revoked(_))
        ));
    }

    /// Answers are judged against one settled state: an answer that passed
    /// under the pending state (a fixed form) is refused once discovery
    /// learns a prefix over it, though another answer was what waited (#46
    /// adversarial review, P1).
    #[tokio::test]
    async fn answers_accepted_before_nat64_settles_are_judged_again() {
        let fixed = "2002:808:808:7f00:0:100::";
        let waiting = "2a00:1:2:3:a9:fea9:fe00:0";
        let table = Table::with(&[("mixed.example", Ok(vec![fixed, waiting]))]);
        let (publisher, handle) = Nat64Handle::channel(Nat64State::Pending);
        let resolver = GateResolver::new(table, NetworkCapability::new(), Some(handle));
        let mut resolving =
            tokio::spawn(async move { resolver.resolve_host("mixed.example").await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut resolving)
                .await
                .is_err()
        );
        let overlap = Nat64Prefix::new("2002:808:808::".parse().unwrap(), 48).unwrap();
        let waiting_prefix = Nat64Prefix::new("2a00:1:2:3::".parse().unwrap(), 64).unwrap();
        publisher.publish(Nat64State::known([overlap, waiting_prefix]));
        let result = resolving.await.unwrap();
        assert!(
            matches!(result, Err(ResolveRefused::AllRefused(_))),
            "both answers are refused in the settled state, got {result:?}"
        );
    }

    #[tokio::test]
    async fn ipv6_answers_wait_for_nat64_discovery() {
        let table = Table::with(&[("v6.example", Ok(vec!["2a00:1:2:3:a9:fea9:fe00:0"]))]);
        let (publisher, handle) = Nat64Handle::channel(Nat64State::Pending);
        let resolver = GateResolver::new(table.clone(), NetworkCapability::new(), Some(handle));
        let pending = tokio::spawn(async move { resolver.resolve_host("v6.example").await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!pending.is_finished(), "the lookup waits for discovery");
        // Discovery learns the /64 whose embedding is 169.254.169.254.
        let prefix = Nat64Prefix::new("2a00:1:2:3::".parse().unwrap(), 64).unwrap();
        publisher.publish(Nat64State::known([prefix]));
        assert!(matches!(
            pending.await.unwrap(),
            Err(ResolveRefused::AllRefused(AddressRefusal::Embedded { .. }))
        ));
    }

    #[test]
    fn its_texts_never_say_timeout() {
        for err in [
            ResolveRefused::Revoked(Revoked),
            ResolveRefused::Onion,
            ResolveRefused::Empty,
            ResolveRefused::AllRefused(AddressRefusal::Nat64Pending),
            ResolveRefused::AllRefused(AddressRefusal::NotGlobalUnicast),
        ] {
            let text = err.to_string().to_ascii_lowercase();
            assert!(
                !text.contains("timeout") && !text.contains("timed out"),
                "{text}"
            );
        }
    }
}
