//! Native data plane. Node owns network key derivation, AEAD and application signatures;
//! this process transports opaque sealed frames and verified BLAKE3 blobs only.
use std::{collections::HashMap, path::PathBuf, sync::{Arc, atomic::{AtomicU64, Ordering}}, time::Duration};
use anyhow::{bail, Context, Result};
use futures_lite::StreamExt;
use iroh::{endpoint::Connection, protocol::{ProtocolHandler, Router}, Endpoint, NodeAddr};
use iroh_blobs::{net_protocol::Blobs, rpc::client::blobs::WrapOption,
    store::{mem::Store, ExportFormat, ExportMode}, ticket::BlobTicket, util::SetTagOption};
use iroh_gossip::{net::{Event, Gossip, GossipEvent, GossipSender, GOSSIP_ALPN}, proto::TopicId};
use n0_future::boxed::BoxFuture;
use serde_json::{json, Value};
use tokio::{sync::{mpsc, oneshot, Mutex}, task::JoinHandle};

const ALPN: &[u8] = b"the-dealer/sealed-frame/1";
const MAX_FRAME: usize = 8 * 1024 * 1024;
type Replies = Arc<Mutex<HashMap<u64, oneshot::Sender<Vec<u8>>>>>;

#[derive(Debug, Clone)]
struct Direct {
    events: mpsc::Sender<Value>, replies: Replies, sequence: Arc<AtomicU64>,
}
impl ProtocolHandler for Direct {
    fn accept(&self, connection: Connection) -> BoxFuture<Result<()>> {
        let this = self.clone();
        Box::pin(async move {
            let (mut send, mut recv) = connection.accept_bi().await?;
            let data = tokio::time::timeout(Duration::from_secs(30), recv.read_to_end(MAX_FRAME)).await??;
            let id = this.sequence.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = oneshot::channel();
            this.replies.lock().await.insert(id, tx);
            let result = async {
                this.events.send(json!({"type":"unicast", "requestId":id,
                    "senderNodeId":connection.remote_node_id()?.to_string(), "data":data})).await?;
                let response = tokio::time::timeout(Duration::from_secs(30), rx).await??;
                send.write_all(&response).await?;
                send.finish()?;
                Ok::<_, anyhow::Error>(())
            }.await;
            this.replies.lock().await.remove(&id);
            result?;
            connection.closed().await;
            Ok(())
        })
    }
}
struct Topic { sender: GossipSender, receive: JoinHandle<()> }

pub struct Engine {
    endpoint: Endpoint, router: Router, gossip: Gossip, blobs: Blobs<Store>,
    topics: Mutex<HashMap<String, Topic>>, events_tx: mpsc::Sender<Value>,
    events_rx: Mutex<mpsc::Receiver<Value>>, replies: Replies,
}
impl Engine {
    pub async fn new() -> Result<Self> {
        let endpoint = Endpoint::builder().discovery_n0().bind().await?;
        let gossip = Gossip::builder().spawn(endpoint.clone()).await?;
        let blobs = Blobs::memory().build(&endpoint);
        let (events_tx, events_rx) = mpsc::channel(256);
        let replies = Arc::new(Mutex::new(HashMap::new()));
        let direct = Direct { events: events_tx.clone(), replies: replies.clone(), sequence: Arc::new(AtomicU64::new(1)) };
        let router = Router::builder(endpoint.clone())
            .accept(GOSSIP_ALPN, gossip.clone())
            .accept(iroh_blobs::ALPN, blobs.clone())
            .accept(ALPN, direct).spawn();
        Ok(Self { endpoint, router, gossip, blobs, topics: Mutex::new(HashMap::new()), events_tx, events_rx: Mutex::new(events_rx), replies })
    }
    /// JSON-RPC methods use byte arrays so no extra encoding dependency is required.
    /// `poll` is nonblocking; callers must unseal all returned control data before use.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        match method {
            "identity" => Ok(serde_json::to_value(self.endpoint.node_addr().await?)?),
            "mount" => {
                let topic = string(&params, "swarmTopicId")?.to_owned();
                let mut topics = self.topics.lock().await;
                if topics.contains_key(&topic) { bail!("topic already mounted"); }
                let bytes = decode_hash(&topic)?;
                let peers: Vec<NodeAddr> = serde_json::from_value(params.get("peers").cloned().unwrap_or(json!([])))?;
                let ids = peers.iter().map(|p| p.node_id).collect();
                for peer in peers { self.endpoint.add_node_addr(peer)?; }
                let (sender, mut receiver) = self.gossip.subscribe(TopicId::from_bytes(bytes), ids)?.split();
                let tx = self.events_tx.clone(); let name = topic.clone();
                let receive = tokio::spawn(async move {
                    while let Ok(Some(event)) = receiver.try_next().await {
                        if let Event::Gossip(GossipEvent::Received(message)) = event {
                            if tx.send(json!({"type":"gossip", "swarmTopicId":name,
                                "data":message.content.to_vec()})).await.is_err() { break; }
                        }
                    }
                });
                topics.insert(topic, Topic { sender, receive }); Ok(json!(true))
            }
            "unmount" => {
                if let Some(topic) = self.topics.lock().await.remove(string(&params, "swarmTopicId")?) { topic.receive.abort(); }
                Ok(json!(true))
            }
            "broadcast" => {
                let sender = self.topics.lock().await.get(string(&params, "swarmTopicId")?).context("unknown topic")?.sender.clone();
                sender.broadcast(data(&params)?.into()).await?; Ok(json!(true))
            }
            "unicast" => {
                let addr: NodeAddr = serde_json::from_value(params["peer"].clone())?;
                let bytes = data(&params)?;
                let connection = self.endpoint.connect(addr, ALPN).await?;
                let result = async {
                    let (mut send, mut recv) = connection.open_bi().await?;
                    send.write_all(&bytes).await?; send.finish()?;
                    Ok::<_, anyhow::Error>(recv.read_to_end(MAX_FRAME).await?)
                }.await;
                connection.close(0u32.into(), b"complete");
                Ok(json!(result?))
            }
            "reply" => {
                let id = params["requestId"].as_u64().context("requestId required")?;
                let bytes = data(&params)?;
                self.replies.lock().await.remove(&id).context("unknown or expired request")?
                    .send(bytes).map_err(|_| anyhow::anyhow!("request closed"))?;
                Ok(json!(true))
            }
            "poll" => {
                let mut events = Vec::new();
                let mut receiver = self.events_rx.lock().await;
                while let Ok(event) = receiver.try_recv() { events.push(event); }
                Ok(json!(events))
            }
            "shareFile" => {
                let path = std::path::absolute(string(&params, "path")?)?;
                let blob = self.blobs.client().add_from_path(path.clone(), false, SetTagOption::Auto, WrapOption::NoWrap).await?.finish().await?;
                let ticket = BlobTicket::new(self.endpoint.node_addr().await?, blob.hash, blob.format)?;
                Ok(json!({"blake3Hash":blob.hash.to_string(), "ticket":ticket.to_string(),
                    "sizeBytes":tokio::fs::metadata(&path).await?.len(), "name":path.file_name().and_then(|n| n.to_str())}))
            }
            "fetchBlobToPath" => {
                let ticket: BlobTicket = string(&params, "ticket")?.parse()?;
                let dest = std::path::absolute(string(&params, "path")?)?;
                self.blobs.client().download(ticket.hash(), ticket.node_addr().clone()).await?.finish().await?;
                let parent = dest.parent().context("destination has no parent")?;
                tokio::fs::create_dir_all(parent).await?;
                let temp: PathBuf = parent.join(format!(".dealer-{}-{}.part", std::process::id(), ticket.hash()));
                let result = async {
                    self.blobs.client().export(ticket.hash(), temp.clone(), ExportFormat::Blob, ExportMode::Copy).await?.finish().await?;
                    #[cfg(unix)] {
                        use std::os::unix::fs::PermissionsExt;
                        tokio::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o600)).await?;
                    }
                    tokio::fs::rename(&temp, &dest).await?;
                    Ok::<_, anyhow::Error>(json!(true))
                }.await;
                let _ = tokio::fs::remove_file(temp).await; result
            }
            "hash" => Ok(json!(blake3::hash(&data(&params)?).to_hex().to_string())),
            _ => bail!("unknown method: {method}"),
        }
    }
    pub async fn shutdown(self) -> Result<()> {
        for topic in self.topics.into_inner().values() { topic.receive.abort(); }
        self.router.shutdown().await
    }
}
fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> { value[field].as_str().with_context(|| format!("missing {field}")) }
fn data(value: &Value) -> Result<Vec<u8>> {
    let bytes: Vec<u8> = serde_json::from_value(value["data"].clone())?;
    if bytes.len() > MAX_FRAME { bail!("frame exceeds limit"); } Ok(bytes)
}
fn decode_hash(input: &str) -> Result<[u8; 32]> {
    if input.len() != 64 || !input.bytes().all(|b| b.is_ascii_hexdigit()) { bail!("expected 32-byte hex topic"); }
    let mut bytes = [0; 32];
    for (i, byte) in bytes.iter_mut().enumerate() { *byte = u8::from_str_radix(&input[i * 2..i * 2 + 2], 16)?; }
    Ok(bytes)
}
