use std::sync::Arc;
use anyhow::Result;
use dealer_iroh_engine::Engine;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, sync::{mpsc, Semaphore}, task::JoinSet};

#[derive(Deserialize)]
struct Request { jsonrpc: String, id: Value, method: String, #[serde(default)] params: Value }

/// stdout is reserved for JSON-RPC. Concurrent requests let poll/reply complete
/// while outgoing QUIC calls wait for their remote application response.
#[tokio::main]
async fn main() -> Result<()> {
    let engine = Arc::new(Engine::new().await?);
    let mut stdin = BufReader::new(tokio::io::stdin());
    let (tx, mut rx) = mpsc::channel::<Value>(64);
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(response) = rx.recv().await {
            stdout.write_all(serde_json::to_string(&response)?.as_bytes()).await?;
            stdout.write_all(b"\n").await?; stdout.flush().await?;
        }
        Ok::<_, anyhow::Error>(())
    });
    let permits = Arc::new(Semaphore::new(64));
    let mut tasks = JoinSet::new();
    loop {
        while tasks.try_join_next().is_some() {}
        let mut line = Vec::new();
        loop {
            let buffer = stdin.fill_buf().await?;
            if buffer.is_empty() { break; }
            let count = buffer.iter().position(|b| *b == b'\n').map_or(buffer.len(), |i| i + 1);
            if line.len() + count > 48 * 1024 * 1024 { anyhow::bail!("RPC line exceeds limit"); }
            line.extend_from_slice(&buffer[..count]); stdin.consume(count);
            if line.last() == Some(&b'\n') { break; }
        }
        if line.is_empty() { break; }
        let permit = permits.clone().acquire_owned().await?;
        let engine = engine.clone(); let tx = tx.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let response = match serde_json::from_slice::<Request>(&line) {
                Ok(request) if request.jsonrpc == "2.0" => {
                    match tokio::time::timeout(std::time::Duration::from_secs(60), engine.call(&request.method, request.params)).await {
                        Ok(Ok(result)) => json!({"jsonrpc":"2.0", "id":request.id, "result":result}),
                        result => {
                            let message = match result { Ok(Err(e)) => e.to_string(), _ => "operation timed out".into() };
                            json!({"jsonrpc":"2.0", "id":request.id, "error":{"code":-32000,"message":message}})
                        }
                    }
                }
                _ => json!({"jsonrpc":"2.0", "id":null, "error":{"code":-32600,"message":"invalid request"}}),
            };
            let _ = tx.send(response).await;
        });
    }
    while tasks.join_next().await.is_some() {}
    drop(tx); writer.await??;
    Arc::try_unwrap(engine).map_err(|_| anyhow::anyhow!("engine still in use"))?.shutdown().await
}
