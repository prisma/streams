//! The s3lite emulator served in-process on a loopback port, compiled from
//! the binary's own source, with an HTTP fault layer in front of it. The
//! layer answers 500 either instead of forwarding a PUT or after the
//! emulator applied it, so the production client's own retry of a
//! conditional PUT is exercised, which a wrapper above the client cannot
//! reach.
#![cfg(test)]

use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{HeaderName, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

#[path = "../../../bin/s3lite/emulator.rs"]
mod emulator;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum HttpFault {
    /// Answer 500 without forwarding the PUT.
    FailBeforeCommit,
    /// Forward the PUT, let the emulator apply it, then answer 500.
    LoseReplyAfterCommit,
}

#[derive(Debug)]
struct Armed {
    fault: HttpFault,
    precondition: HeaderName,
    path_contains: String,
}

pub(super) struct HttpFaults {
    armed: Mutex<Option<Armed>>,
    fired: AtomicU64,
}

impl HttpFaults {
    /// Apply `fault` to the next PUT carrying `precondition` (`If-Match`
    /// for updates, `If-None-Match` for creates) whose path contains
    /// `path_contains`.
    pub(super) fn arm(&self, fault: HttpFault, precondition: HeaderName, path_contains: &str) {
        *self.armed.lock().unwrap() = Some(Armed {
            fault,
            precondition,
            path_contains: path_contains.into(),
        });
    }

    pub(super) fn fired(&self) -> u64 {
        self.fired.load(SeqCst)
    }

    fn take(&self, request: &Request) -> Option<HttpFault> {
        let mut armed = self.armed.lock().unwrap();
        let hit = armed.as_ref().is_some_and(|a| {
            request.method() == Method::PUT
                && request.headers().contains_key(&a.precondition)
                && request.uri().path().contains(&a.path_contains)
        });
        if !hit {
            return None;
        }
        self.fired.fetch_add(1, SeqCst);
        armed.take().map(|a| a.fault)
    }
}

fn server_error() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "injected by the provider contract",
    )
        .into_response()
}

async fn fault_layer(
    State(faults): State<Arc<HttpFaults>>,
    request: Request,
    next: Next,
) -> Response {
    match faults.take(&request) {
        None => next.run(request).await,
        Some(HttpFault::FailBeforeCommit) => server_error(),
        Some(HttpFault::LoseReplyAfterCommit) => {
            let applied = next.run(request).await;
            assert!(
                applied.status().is_success(),
                "the armed PUT did not apply: {}",
                applied.status()
            );
            server_error()
        }
    }
}

pub(super) struct S3lite {
    listener: tokio::net::TcpListener,
    app: Arc<emulator::AppState>,
    faults: Arc<HttpFaults>,
}

impl S3lite {
    pub(super) async fn bind() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback port for s3lite");
        S3lite {
            listener,
            app: emulator::AppState::new(Duration::from_millis(2), None),
            faults: Arc::new(HttpFaults {
                armed: Mutex::new(None),
                fired: AtomicU64::new(0),
            }),
        }
    }

    pub(super) fn endpoint(&self) -> String {
        let address = self.listener.local_addr().expect("s3lite address");
        format!("http://{address}")
    }

    pub(super) fn faults(&self) -> Arc<HttpFaults> {
        self.faults.clone()
    }

    /// Serve until `work` finishes; the server stops with it.
    pub(super) async fn serve_while<T>(self, work: impl std::future::Future<Output = T>) -> T {
        let router = emulator::router(self.app).layer(axum::middleware::from_fn_with_state(
            self.faults,
            fault_layer,
        ));
        let listener = self.listener;
        tokio::select! {
            served = async move { axum::serve(listener, router).await } => {
                panic!("the in-process s3lite stopped: {served:?}")
            }
            done = work => done,
        }
    }
}
