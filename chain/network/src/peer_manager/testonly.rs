use crate::broadcast;
use crate::config;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Encoding, PeerInfo, PeerMessage, SignedAccountData, SyncAccountsData,
};
use crate::peer;
use crate::peer::peer_actor::ClosingReason;
use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::tcp;
use crate::test_utils;
use crate::testonly::actix::ActixSystem;
use crate::testonly::fake_client;
use crate::time;
use crate::types::{
    ChainInfo, KnownPeerStatus, PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo,
};
use crate::PeerManagerActor;
use near_o11y::{WithSpanContext, WithSpanContextExt};
use near_primitives::network::PeerId;
use near_primitives::types::EpochId;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(actix::Message)]
#[rtype("()")]
struct WithNetworkState(
    Box<dyn Send + FnOnce(Arc<NetworkState>) -> Pin<Box<dyn Send + 'static + Future<Output = ()>>>>,
);

impl actix::Handler<WithNetworkState> for PeerManagerActor {
    type Result = ();
    fn handle(
        &mut self,
        WithNetworkState(f): WithNetworkState,
        _: &mut Self::Context,
    ) -> Self::Result {
        assert!(actix::Arbiter::current().spawn(f(self.state.clone())));
    }
}

#[derive(actix::Message, Debug)]
#[rtype("()")]
struct CheckConsistency;

impl actix::Handler<WithSpanContext<CheckConsistency>> for PeerManagerActor {
    type Result = ();
    /// Checks internal consistency of the PeerManagerActor.
    /// This is a partial implementation, add more invariant checks
    /// if needed.
    fn handle(&mut self, _: WithSpanContext<CheckConsistency>, _: &mut actix::Context<Self>) {
        // Check that the set of ready connections matches the PeerStore state.
        let tier2: HashSet<_> = self.state.tier2.load().ready.keys().cloned().collect();
        let store: HashSet<_> = self
            .state
            .peer_store
            .dump()
            .into_iter()
            .filter_map(|state| {
                if state.status == KnownPeerStatus::Connected {
                    Some(state.peer_info.id)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(tier2, store);
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Event {
    Client(fake_client::Event),
    PeerManager(PME),
}

pub(crate) struct ActorHandler {
    pub cfg: config::NetworkConfig,
    pub events: broadcast::Receiver<Event>,
    pub actix: ActixSystem<PeerManagerActor>,
}

pub fn unwrap_sync_accounts_data_processed(ev: Event) -> Option<SyncAccountsData> {
    match ev {
        Event::PeerManager(PME::MessageProcessed(
            tcp::Tier::T2,
            PeerMessage::SyncAccountsData(msg),
        )) => Some(msg),
        _ => None,
    }
}

pub(crate) fn make_chain_info(
    epoch_id: &EpochId,
    chain: &data::Chain,
    validators: &[&ActorHandler],
) -> ChainInfo {
    // Construct ChainInfo with tier1_accounts set to `validators`.
    let vs: Vec<_> = validators.iter().map(|pm| pm.cfg.validator.clone().unwrap()).collect();
    let account_keys = Arc::new(
        vs.iter()
            .map(|v| ((epoch_id.clone(), v.signer.validator_id().clone()), v.signer.public_key()))
            .collect(),
    );
    let mut chain_info = chain.get_chain_info();
    chain_info.tier1_accounts = account_keys;
    chain_info
}

pub(crate) struct RawConnection {
    events: broadcast::Receiver<Event>,
    stream: tcp::Stream,
    cfg: peer::testonly::PeerConfig,
}

impl RawConnection {
    pub async fn handshake(mut self, clock: &time::Clock) -> peer::testonly::PeerHandle {
        let stream_id = self.stream.id();
        let mut peer =
            peer::testonly::PeerHandle::start_endpoint(clock.clone(), self.cfg, self.stream).await;

        // Wait for the new peer to complete the handshake.
        peer.complete_handshake().await;

        // Wait for the peer manager to complete the handshake.
        self.events
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::HandshakeCompleted(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    panic!("handshake aborted: {}", ev.reason)
                }
                _ => None,
            })
            .await;
        peer
    }

    // Try to perform a handshake. PeerManager is expected to reject the handshake.
    pub async fn manager_fail_handshake(mut self, clock: &time::Clock) -> ClosingReason {
        let stream_id = self.stream.id();
        let peer =
            peer::testonly::PeerHandle::start_endpoint(clock.clone(), self.cfg, self.stream).await;
        let reason = self
            .events
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    Some(ev.reason)
                }
                Event::PeerManager(PME::HandshakeCompleted(ev)) if ev.stream_id == stream_id => {
                    panic!("PeerManager accepted the handshake")
                }
                _ => None,
            })
            .await;
        drop(peer);
        reason
    }
}

impl ActorHandler {
    pub fn peer_info(&self) -> PeerInfo {
        PeerInfo {
            id: PeerId::new(self.cfg.node_key.public_key()),
            addr: self.cfg.node_addr.clone(),
            account_id: None,
        }
    }

    pub async fn connect_to(&self, peer_info: &PeerInfo, tier: tcp::Tier) {
        let stream = tcp::Stream::connect(peer_info, tier).await.unwrap();
        let mut events = self.events.from_now();
        let stream_id = stream.id();
        self.actix
            .addr
            .do_send(PeerManagerMessageRequest::OutboundTcpConnect(stream).with_span_context());
        events
            .recv_until(|ev| match &ev {
                Event::PeerManager(PME::HandshakeCompleted(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    panic!("PeerManager rejected the handshake")
                }
                _ => None,
            })
            .await;
    }

    pub async fn with_state<R: 'static + Send, Fut: 'static + Send + Future<Output = R>>(
        &self,
        f: impl 'static + Send + FnOnce(Arc<NetworkState>) -> Fut,
    ) -> R {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.actix
            .addr
            .send(WithNetworkState(Box::new(|s| {
                Box::pin(async { send.send(f(s).await).ok().unwrap() })
            })))
            .await
            .unwrap();
        recv.await.unwrap()
    }

    pub async fn start_inbound(
        &self,
        chain: Arc<data::Chain>,
        network_cfg: config::NetworkConfig,
    ) -> RawConnection {
        // To avoid race condition:
        // 1. reserve a TCP port
        // 2. snapshot event stream
        // 3. establish connection.
        let socket = tcp::Socket::bind_v4();
        let events = self.events.from_now();
        let stream = socket.connect(&self.peer_info(), tcp::Tier::T2).await;
        let stream_id = stream.id();
        let conn = RawConnection {
            events,
            stream,
            cfg: peer::testonly::PeerConfig {
                network: network_cfg,
                chain,
                force_encoding: Some(Encoding::Proto),
                nonce: None,
            },
        };
        // Wait until the TCP connection is accepted or rejected.
        // The Handshake is not performed yet.
        conn.events
            .clone()
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::HandshakeStarted(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                _ => None,
            })
            .await;
        conn
    }

    pub async fn start_outbound(
        &self,
        chain: Arc<data::Chain>,
        network_cfg: config::NetworkConfig,
        tier: tcp::Tier,
    ) -> RawConnection {
        let (outbound_stream, inbound_stream) =
            tcp::Stream::loopback(network_cfg.node_id(), tier).await;
        let stream_id = outbound_stream.id();
        let events = self.events.from_now();
        self.actix.addr.do_send(
            PeerManagerMessageRequest::OutboundTcpConnect(outbound_stream).with_span_context(),
        );
        let conn = RawConnection {
            events,
            stream: inbound_stream,
            cfg: peer::testonly::PeerConfig {
                network: network_cfg,
                chain,
                force_encoding: Some(Encoding::Proto),
                nonce: None,
            },
        };
        // Wait until the handshake started or connection is closed.
        // The Handshake is not performed yet.
        conn.events
            .clone()
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::HandshakeStarted(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                _ => None,
            })
            .await;
        conn
    }

    pub async fn check_consistency(&self) {
        self.actix.addr.send(CheckConsistency.with_span_context()).await.unwrap();
    }

    pub async fn set_chain_info(&self, chain_info: ChainInfo) {
        self.actix.addr.send(SetChainInfo(chain_info).with_span_context()).await.unwrap();
    }

    pub async fn tier1_advertise_proxies(
        &self,
        clock: &time::Clock,
    ) -> Vec<Arc<SignedAccountData>> {
        let clock = clock.clone();
        self.with_state(move |s| async move { s.tier1_advertise_proxies(&clock).await }).await
    }

    // Awaits until the accounts_data state matches `want`.
    pub async fn wait_for_accounts_data(&self, want: &HashSet<Arc<SignedAccountData>>) {
        let mut events = self.events.from_now();
        loop {
            let got = self
                .with_state(move |s| async move {
                    s.accounts_data.load().data.values().cloned().collect::<HashSet<_>>()
                })
                .await;
            if &got == want {
                break;
            }
            // It is important that we wait for the next PeerMessage::SyncAccountsData to get
            // PROCESSED, not just RECEIVED. Otherwise we would get a race condition.
            events.recv_until(unwrap_sync_accounts_data_processed).await;
        }
    }

    // Awaits until the routing_table matches `want`.
    pub async fn wait_for_routing_table(&self, want: &[(PeerId, Vec<PeerId>)]) {
        let mut events = self.events.from_now();
        loop {
            let resp = self
                .actix
                .addr
                .send(PeerManagerMessageRequest::FetchRoutingTable.with_span_context())
                .await
                .unwrap();
            let got = match resp {
                PeerManagerMessageResponse::FetchRoutingTable(rt) => rt.next_hops,
                _ => panic!("bad response"),
            };
            if test_utils::expected_routing_tables(&got, want) {
                return;
            }
            events
                .recv_until(|ev| match ev {
                    Event::PeerManager(PME::RoutingTableUpdate { .. }) => Some(()),
                    _ => None,
                })
                .await;
        }
    }

    pub async fn tier1_connect(&self, clock: &time::Clock) {
        let clock = clock.clone();
        self.with_state(move |s| async move {
            s.tier1_connect(&clock).await;
        })
        .await;
    }
}

pub(crate) async fn start(
    clock: time::Clock,
    store: Arc<dyn near_store::db::Database>,
    cfg: config::NetworkConfig,
    chain: Arc<data::Chain>,
) -> ActorHandler {
    let (send, recv) = broadcast::unbounded_channel();
    let actix = ActixSystem::spawn({
        let mut cfg = cfg.clone();
        let chain = chain.clone();
        move || {
            let genesis_id = chain.genesis_id.clone();
            let fc = Arc::new(fake_client::Fake { event_sink: send.sink().compose(Event::Client) });
            cfg.event_sink = send.sink().compose(Event::PeerManager);
            PeerManagerActor::spawn(clock, store, cfg, fc, genesis_id).unwrap()
        }
    })
    .await;
    let mut h = ActorHandler { cfg, actix, events: recv };
    // Wait for the server to start.
    assert_eq!(Event::PeerManager(PME::ServerStarted), h.events.recv().await);
    h.actix.addr.send(SetChainInfo(chain.get_chain_info()).with_span_context()).await.unwrap();
    h
}
