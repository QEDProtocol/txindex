use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Method;
use hyper::Request;
use hyper::Response;
use hyper::StatusCode;
use hyper_util::rt::TokioIo;
use log::warn;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use txindex_common::config::Config;

use crate::api::core::HttpError;
use crate::daemon::query::Query;

use super::chain::chain_handle_request;
use super::traits::TxIndexRESTHandler;


type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

#[derive(Clone)]
pub struct RestServerHandler<API: 'static + TxIndexRESTHandler + Clone> {
    pub query: Arc<Query>,
    pub config: Arc<Config>,
    _api: PhantomData<API>,
}
impl<API: 'static + TxIndexRESTHandler + Clone> RestServerHandler<API>{
    pub fn new(query: Arc<Query>, config: Arc<Config>) -> Self {
        Self {
            query,
            config,
            _api: PhantomData,
        }
    }
pub async fn handle(
    &self,
    req: Request<hyper::body::Incoming>,
) -> anyhow::Result<Response<BoxBody>> {
    if req.method() == Method::OPTIONS {
        return preflight(req).await;
    }
    let method = req.method().clone();
    let uri = req.uri().clone();
    let body = req.collect().await?.to_bytes();
    let query = Arc::clone(&self.query);
    let config = Arc::clone(&self.config);
    if uri.path().starts_with("/indexer/") {


    let resp = API::handle_request(method, uri, body, query, config)
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
        Ok(resp)

    }else{
        let res = chain_handle_request(method, uri, body, &query, &config);
        match res {
            Ok(v) => Ok(v),
            Err(err) => {
                warn!("{:?}", err);
                Ok(Response::builder()
                    .status(err.0)
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Content-Type", "application/json")
                    .body(full(
                        err
                        .to_json_bytes()
                        .unwrap(),
                    ))
                    .unwrap())
            }
        }
    }
}
}
pub async fn preflight(req: Request<Incoming>) -> anyhow::Result<Response<BoxBody>> {
    let _whole_body = req.collect().await?;
    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Headers", "*")
        .header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        .body(BoxBody::default())?;
    Ok(response)
}
#[tokio::main]
pub async fn run<API: 'static + TxIndexRESTHandler + Clone + Send + Sync>(config: Arc<Config>, query: Arc<Query>, rx: oneshot::Receiver<()>) -> anyhow::Result<()> {
    let handler = RestServerHandler::<API>::new(query, config);
    let addr: SocketAddr = SocketAddr::from(handler.config.http_addr);

    let listener = TcpListener::bind(addr).await?;
    log::info!("Listening on http://{}", addr);
    let handler =handler.clone();
    //let graceful = hyper_util::server::graceful::GracefulShutdown::new();

    let mut signal = std::pin::pin!(async {
        rx.await.ok();
    });

    loop {
        tokio::select! {
            Ok((stream, _addr)) = listener.accept() => {
                
        let io = TokioIo::new(stream);
        let handler = handler.clone();

        tokio::task::spawn(async move {
            let service = service_fn(|req| async { handler.handle(req).await });

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                      log::info!("Failed to serve connection: {:?}", err);
            }
        });
            },
    
            _ = &mut signal => {
                eprintln!("graceful shutdown signal received");
                // stop the accept loop
                break;
            }
        }
    };
    Ok(())

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


pub fn start<API: 'static + TxIndexRESTHandler + Clone + Send + Sync>(config: Arc<Config>, query: Arc<Query>) -> Handle {
    let (tx, rx) = oneshot::channel::<()>();

    Handle {
        tx,
        thread: thread::spawn(move || {
            run::<API>(config, query, rx).unwrap()
        }),
    }
}

pub fn full<T: Into<Bytes>>(chunk: T) -> BoxBody {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}
