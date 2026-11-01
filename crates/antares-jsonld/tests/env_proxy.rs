// SPDX-License-Identifier: EUPL-1.2
//! `client_builder` and the proxy environment. A proxy resolves the target
//! name itself (absolute-form GET, CONNECT), so a client that picks up
//! `HTTP_PROXY` from the environment never asks `PolicyResolver` about the
//! target: the DNS pin and the private-range deny would only ever see the
//! proxy's own name. Its own binary because it sets the process
//! environment, which every other test building a client would read.
#![cfg(feature = "fetch")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ngsild_jsonld::loader::{client_builder, EgressPolicy};
use std::io::Read;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// With private egress denied, a name that resolves to loopback is refused
/// by the client's own resolver — and an environment proxy must not take
/// the request instead, to resolve the name where the policy cannot see it.
#[tokio::test]
async fn an_environment_proxy_does_not_take_the_policy_out_of_the_path() {
    let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = proxy.local_addr().unwrap();
    let reached = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&reached);
    std::thread::spawn(move || {
        if let Ok((mut s, _)) = proxy.accept() {
            let mut buf = [0u8; 256];
            let _ = s.read(&mut buf);
            seen.store(true, Ordering::SeqCst);
        }
    });
    for k in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        std::env::set_var(k, format!("http://{addr}"));
    }
    for k in ["NO_PROXY", "no_proxy", "REQUEST_METHOD"] {
        std::env::remove_var(k);
    }
    let policy = EgressPolicy {
        allow_private: false,
        private_hosts: &[],
    };
    let client = client_builder(policy)
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .unwrap();
    let res = client.get("http://localhost:9/ctx.jsonld").send().await;
    assert!(res.is_err(), "a loopback target was fetched: {res:?}");
    assert!(
        !reached.load(Ordering::SeqCst),
        "the request went to the environment proxy, past the DNS pin"
    );
}
