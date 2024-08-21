/*use std::{fs, net::SocketAddr, os::unix::fs::FileTypeExt, sync::Arc};

use http_body_util::{BodyExt, Full};
use hyper::{body::Bytes, service::service_fn, Response, StatusCode};
use hyper_util::rt::TokioIo;
use log::{info, warn};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{net::TcpListener, sync::oneshot};
use txindex_common::config::Config;

use crate::api::core::HttpError;

use super::traits::{BoxBody, TxIndexRESTHandler};
use hyper::server::conn::http1;

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

pub fn create_socket(addr: &SocketAddr) -> Socket {
    let domain = match &addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let socket =
        Socket::new(domain, Type::STREAM, Some(Protocol::TCP)).expect("creating socket failed");

    #[cfg(unix)]
    socket
        .set_reuse_port(true)
        .expect("cannot enable SO_REUSEPORT");

    socket.bind(&addr.clone().into()).expect("cannot bind");

    socket
}

#[derive(Clone)]
pub struct NTSend<Q: Send + Sync + Clone> {
    pub query: Arc<Q>,
    pub config: Arc<Config>,
}

#[tokio::main]
async fn run_server<'a, API: TxIndexRESTHandler<Q>, Q: Send + Sync + Clone>(
    nts: Arc<NTSend<Q>>,
    rx: oneshot::Receiver<()>,
) where
    Arc<Q>: Send + Sync,
{
  let nts = nts.clone();
    let addr = &nts.config.http_addr;
    let socket_file = &nts.config.http_socket_file;


    let socket = create_socket(&addr);
    socket.listen(511).expect("setting backlog failed");
    let listener = TcpListener::from_std(socket.into());
    if listener.is_err() {
        eprintln!("failed to bind to address: {}", addr);
        return;
    }
    let listener = listener.unwrap();

    // specify our HTTP settings (http1, http2, auto all work)
    let mut http = http1::Builder::new();
    // the graceful watcher
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    // when this signal completes, start shutdown
    let mut signal = std::pin::pin!(async {
        rx.await.ok();
    });
    let query: Arc<Q> = nts.query.clone();
    let config: Arc<Config> = nts.config.clone();

    // Our server accept loop
    loop {
        let query: Arc<Q> = Arc::clone(&query);
        let config: Arc<Config> = Arc::clone(&config);
        tokio::select! {
            Ok((stream, _addr)) = listener.accept() => {
                let io = TokioIo::new(stream);
                let conn = http
                .serve_connection(
                    io,
                    service_fn(move |req| {
                        let query = Arc::clone(&query);
                        let config = Arc::clone(&config);

                        async move {
                            let method = req.method().clone();
                            let uri = req.uri().clone();
                            let body = req.collect().await?.to_bytes();

                            let mut resp = API::handle_request(method, uri, body, &query, &config)
                                .unwrap_or_else(|err| {
                                    warn!("{:?}", err);
                                    let p = Response::builder()
                                        .status(err.0)
                                        .header("Access-Control-Allow-Origin", "*")
                                        .header("Content-Type", "application/json")
                                        .body(full(err.to_json_bytes().unwrap()));

                                    match p {
                                        Ok(v) => v,
                                        Err(e) => {
                                            warn!("{:?}", e);
                                            Response::builder()
                                                .status(500)
                                                .header("Access-Control-Allow-Origin", "*")
                                                .header("Content-Type", "application/json")
                                                .body(full(
                                                    HttpError(
                                                        StatusCode::INTERNAL_SERVER_ERROR,
                                                        "Internal Server Error".to_string(),
                                                    )
                                                    .to_json_bytes()
                                                    .unwrap(),
                                                ))
                                                .unwrap()
                                        }
                                    }
                                });
                            if let Some(ref origins) = config.cors {
                                resp.headers_mut().insert(
                                    "Access-Control-Allow-Origin",
                                    origins.parse().unwrap(),
                                );
                            }
                            Ok::<_, hyper::Error>(resp)
                        }
                    }),
                );
                // watch this connection
                let fut = graceful.watch(conn);
                tokio::spawn(async move {
                    if let Err(e) = fut.await {
                        eprintln!("Error serving connection: {:?}", e);
                    }
                });
            },

            _ = &mut signal => {
                eprintln!("graceful shutdown signal received");
                // stop the accept loop
                break;
            }
        }
    }
}

pub fn start(config: Arc<Config>, query: Arc<Query>) -> Handle {
    let (tx, rx) = oneshot::channel::<()>();

    Handle {
        tx,
        thread: thread::spawn(move || {
            run_server(config, query, rx);
        }),
    }
}

pub struct Handle {
    tx: oneshot::Sender<()>,
    thread: thread::JoinHandle<()>,
}

impl Handle {
    pub fn stop(self) {
        self.tx.send(()).expect("failed to send shutdown signal");
        self.thread.join().expect("REST server failed");
    }
}
*/