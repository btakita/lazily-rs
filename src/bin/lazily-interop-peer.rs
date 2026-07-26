use std::io::{self, BufRead, Write};

use lazily::{
    Context, CrdtOp, CrdtPlaneRuntime, CrdtSync, IpcMessage, IpcValue, NodeId, NodeKey, PeerId,
    WireStamp,
};
use serde_json::{Value, json};

const PROTOCOL_VERSION: u64 = 1;

struct Peer {
    peer_id: Option<PeerId>,
    context: Context,
    runtime: Option<CrdtPlaneRuntime>,
}

impl Peer {
    fn new() -> Self {
        Self {
            peer_id: None,
            context: Context::new(),
            runtime: None,
        }
    }

    fn handle(&mut self, request: &Value) -> Value {
        let Some(command) = request.get("cmd").and_then(Value::as_str) else {
            return error("missing cmd");
        };
        match command {
            "hello" => self.hello(request),
            "local_set" => self.local_set(request),
            "deliver" => self.deliver(request),
            "snapshot" => self.snapshot(),
            "bye" => json!({"ok": true}),
            command if command.starts_with("link_") => json!({
                "ok": false,
                "error": "unsupported channel",
                "unsupported": true
            }),
            _ => error(format!("unknown command {command}")),
        }
    }

    fn hello(&mut self, request: &Value) -> Value {
        if request.get("protocol_version").and_then(Value::as_u64) != Some(PROTOCOL_VERSION) {
            return error(format!(
                "unsupported protocol_version {}",
                request.get("protocol_version").unwrap_or(&Value::Null)
            ));
        }
        let Some(peer) = request.get("peer").and_then(Value::as_u64) else {
            return error("hello requires peer");
        };
        let peer = PeerId(peer);
        self.peer_id = Some(peer);
        self.runtime = Some(CrdtPlaneRuntime::new(peer));
        json!({
            "ok": true,
            "binding": "lazily-rs",
            "version": env!("CARGO_PKG_VERSION"),
            "protocol_version": PROTOCOL_VERSION,
            "features": ["distributed_crdt"],
            "codecs": ["json", "msgpack"],
            "channels": [],
            "channel_variants": {},
            "platform_profile": "portable",
            "carve_outs": ["transport_links"]
        })
    }

    fn local_set(&mut self, request: &Value) -> Value {
        let Some(peer) = self.peer_id else {
            return error("hello must run first");
        };
        let Some(node) = request.get("node").and_then(Value::as_u64) else {
            return error("local_set requires node");
        };
        let key = match request.get("key") {
            Some(Value::Null) => None,
            Some(Value::String(key)) => match NodeKey::new(key) {
                Ok(key) => Some(key),
                Err(error) => return self::error(format!("invalid key: {error}")),
            },
            _ => return error("local_set requires nullable key"),
        };
        let state: IpcValue = match request.get("state").cloned().map(serde_json::from_value) {
            Some(Ok(state)) => state,
            Some(Err(error)) => return self::error(format!("invalid IpcValue: {error}")),
            None => return error("local_set requires state"),
        };
        let Some(at) = request.get("at").and_then(Value::as_u64) else {
            return error("local_set requires at");
        };
        let runtime = match self.runtime.as_mut() {
            Some(runtime) => runtime,
            None => return error("hello must run first"),
        };
        let stamp = runtime.plane_mut().tick(at);
        let wire_stamp = WireStamp::from(stamp);
        let op = match key {
            Some(key) => CrdtOp::keyed(NodeId(node), key, wire_stamp, state),
            None => CrdtOp::new(NodeId(node), wire_stamp, state),
        };
        let sync = CrdtSync::new(vec![(peer.0, wire_stamp)], vec![op]);
        if runtime.ingest(&self.context, &sync, at) != 1 {
            return error("production runtime rejected its fresh local op");
        }
        match serde_json::to_value(IpcMessage::CrdtSync(sync)) {
            Ok(frame) => json!({"ok": true, "frame": frame}),
            Err(error) => self::error(format!("encode CrdtSync: {error}")),
        }
    }

    fn deliver(&mut self, request: &Value) -> Value {
        let Some(frame) = request.get("frame").cloned() else {
            return error("deliver requires frame");
        };
        let message: IpcMessage = match serde_json::from_value(frame) {
            Ok(message) => message,
            Err(error) => return self::error(format!("decode IpcMessage: {error}")),
        };
        let IpcMessage::CrdtSync(sync) = message else {
            return error("deliver requires CrdtSync");
        };
        let Some(at) = request.get("at").and_then(Value::as_u64) else {
            return error("deliver requires at");
        };
        let Some(runtime) = self.runtime.as_mut() else {
            return error("hello must run first");
        };
        let applied = runtime.ingest(&self.context, &sync, at);
        json!({"ok": true, "applied": applied})
    }

    fn snapshot(&self) -> Value {
        let Some(runtime) = self.runtime.as_ref() else {
            return error("hello must run first");
        };
        let cells = runtime
            .converged()
            .into_iter()
            .map(|entry| {
                json!({
                    "node": entry.node.0,
                    "key": entry.key.map(|key| key.as_str().to_owned()),
                    "state": entry.state
                })
            })
            .collect::<Vec<_>>();
        json!({"ok": true, "cells": cells})
    }
}

fn error(message: impl Into<String>) -> Value {
    json!({"ok": false, "error": message.into()})
}

fn self_check() -> Result<(), String> {
    let mut peer = Peer::new();
    let hello = peer.handle(&json!({
        "cmd": "hello",
        "peer": 1,
        "protocol_version": PROTOCOL_VERSION
    }));
    if hello["ok"] != true {
        return Err(format!("hello failed: {hello}"));
    }
    let local = peer.handle(&json!({
        "cmd": "local_set",
        "node": 7,
        "key": null,
        "state": {"Inline": [65]},
        "at": 10
    }));
    if local["ok"] != true || local.pointer("/frame/CrdtSync/ops/0/key") != Some(&Value::Null) {
        return Err(format!("local_set failed canonical key check: {local}"));
    }
    let delivered = peer.handle(&json!({
        "cmd": "deliver",
        "frame": local["frame"],
        "at": 11
    }));
    if delivered["applied"] != 0 {
        return Err(format!(
            "duplicate delivery was not idempotent: {delivered}"
        ));
    }
    let snapshot = peer.handle(&json!({"cmd": "snapshot"}));
    if snapshot.pointer("/cells/0/state/Inline/0") != Some(&json!(65)) {
        return Err(format!("snapshot mismatch: {snapshot}"));
    }
    Ok(())
}

fn main() {
    if std::env::args().any(|argument| argument == "--self-check") {
        match self_check() {
            Ok(()) => {
                println!("lazily-rs interop peer self-check: ok");
                return;
            }
            Err(error) => {
                eprintln!("lazily-rs interop peer self-check: {error}");
                std::process::exit(1);
            }
        }
    }

    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let mut peer = Peer::new();
    for line in stdin.lock().lines() {
        let response = match line {
            Ok(line) => match serde_json::from_str::<Value>(&line) {
                Ok(request) => {
                    let bye = request.get("cmd").and_then(Value::as_str) == Some("bye");
                    let response = peer.handle(&request);
                    if let Err(error) = write_response(&mut stdout, &response) {
                        eprintln!("write response: {error}");
                        std::process::exit(1);
                    }
                    if bye {
                        return;
                    }
                    continue;
                }
                Err(error) => self::error(format!("invalid JSON: {error}")),
            },
            Err(error) => {
                eprintln!("read request: {error}");
                std::process::exit(1);
            }
        };
        if let Err(error) = write_response(&mut stdout, &response) {
            eprintln!("write response: {error}");
            std::process::exit(1);
        }
    }
}

fn write_response(output: &mut impl Write, response: &Value) -> io::Result<()> {
    serde_json::to_writer(&mut *output, response)?;
    output.write_all(b"\n")?;
    output.flush()
}
