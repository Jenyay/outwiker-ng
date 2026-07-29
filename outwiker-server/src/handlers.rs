//! Built-in server-side handlers.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use outwiker_core::protocol::{Request, Response, METHOD_ECHO};
use outwiker_core::{CoreError, CoreResult};

/// Handle to a registered method handler. Cheap to clone so the
/// dispatcher can keep an `Arc` to the chosen handler outside the
/// lock while it executes.
pub type MethodHandler = Arc<dyn Fn(&Server, Request) -> CoreResult<Response> + Send + Sync>;

/// Registry of named method handlers.
pub struct Server {
    handlers: RwLock<HashMap<String, MethodHandler>>,
}

impl Server {
    /// Create an empty server. Built-in handlers (`echo`) are
    /// registered automatically.
    pub fn new() -> Self {
        let s = Self {
            handlers: RwLock::new(HashMap::new()),
        };
        s.register(METHOD_ECHO, |_, req| {
            Ok(Response::ok(req.id, serde_json::json!(req.params)))
        });
        s
    }

    /// Register a new method handler.
    pub fn register<F>(&self, name: impl Into<String>, handler: F)
    where
        F: Fn(&Server, Request) -> CoreResult<Response> + Send + Sync + 'static,
    {
        let handler: MethodHandler = Arc::new(handler);
        self.handlers
            .write()
            .expect("handlers lock poisoned")
            .insert(name.into(), handler);
    }

    /// Look up a handler by method name.
    pub fn get(&self, method: &str) -> Option<MethodHandler> {
        self.handlers
            .read()
            .expect("handlers lock poisoned")
            .get(method)
            .cloned()
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

/// Look up a handler for the request method and invoke it.
pub fn dispatch(server: &Server, request: Request) -> Response {
    match server.get(&request.method) {
        Some(handler) => match handler(server, request.clone()) {
            Ok(resp) => resp,
            Err(CoreError::Rpc(msg)) => Response::err(request.id, msg),
            Err(e) => Response::err(request.id, format!("{e}")),
        },
        None => Response::err(
            request.id,
            format!("unknown method: {}", request.method),
        ),
    }
}
