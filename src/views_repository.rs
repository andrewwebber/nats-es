use std::{io::Cursor, marker::PhantomData};

use async_nats::{jetstream::object_store::GetErrorKind, Client};
use async_trait::async_trait;
use cqrs_es::{
    persist::{PersistenceError, ViewContext, ViewRepository},
    Aggregate, View,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tracing::debug;

#[derive(Clone)]
pub struct NatsViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    client: Client,
    bucket_name: String,
    _phantom: PhantomData<(V, A)>,
}

impl<V, A> NatsViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    pub fn new(client: Client, bucket_name: &str) -> Self {
        let bucket_name = bucket_name.to_string();
        Self {
            client,
            bucket_name,
            _phantom: PhantomData {},
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NatsView {
    /// Unique identifier of the view instance that is being modified.
    pub view_instance_id: String,
    /// The current version of the view instance, used for optimistic locking.
    pub version: i64,
    pub payload: Value,
}

#[async_trait]
impl<V, A> ViewRepository<V, A> for NatsViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    /// Returns the current view instance.
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        let key = format!("{}.{view_id}", A::aggregate_type());
        debug!("load view '{key}'");
        let jetstream = async_nats::jetstream::new(self.client.clone());
        let obj = jetstream
            .get_object_store(&self.bucket_name)
            .await
            .map_err(|e| PersistenceError::ConnectionError(e.into()))?
            .get(key.to_owned())
            .await;

        match obj {
            Ok(mut stream) => {
                debug!("found object store blob {key}");
                let mut buffer = Vec::new();
                stream
                    .read_to_end(&mut buffer)
                    .await
                    .map_err(|e| PersistenceError::DeserializationError(e.into()))?;

                let view: NatsView = serde_json::from_slice(&buffer)?;
                debug!("returing view {view:#?}");
                Ok(Some(serde_json::from_value(view.payload)?))
            }
            Err(e) if e.kind() == GetErrorKind::NotFound => Ok(None),
            Err(e) => Err(PersistenceError::DeserializationError(Box::new(e))),
        }
    }

    /// Returns the current view instance and context, used by the `GenericQuery` to update
    /// views with committed events.
    async fn load_with_context(
        &self,
        view_id: &str,
    ) -> Result<Option<(V, ViewContext)>, PersistenceError> {
        let key = format!("{}.{view_id}", A::aggregate_type());
        debug!("load_with_context - '{key}'");

        let jetstream = async_nats::jetstream::new(self.client.clone());
        let obj = jetstream
            .get_object_store(&self.bucket_name)
            .await
            .map_err(|e| PersistenceError::ConnectionError(e.into()))?
            .get(key.to_owned())
            .await;

        match obj {
            Ok(mut stream) => {
                debug!("found object store blob {key}");
                let mut buffer = Vec::new();
                stream
                    .read_to_end(&mut buffer)
                    .await
                    .map_err(|e| PersistenceError::DeserializationError(e.into()))?;

                let NatsView {
                    view_instance_id,
                    version,
                    payload,
                } = serde_json::from_slice(&buffer)?;

                let context = ViewContext {
                    view_instance_id,
                    version,
                };

                Ok(Some((serde_json::from_value(payload)?, context)))
            }
            Err(e) if e.kind() == GetErrorKind::NotFound => Ok(None),
            Err(e) => Err(PersistenceError::DeserializationError(Box::new(e))),
        }
    }

    /// Updates the view instance and context, used by the `GenericQuery` to update
    /// views with committed events.
    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        let payload = serde_json::to_value(view)?;

        let ViewContext {
            view_instance_id,
            version,
        } = context;

        let view = NatsView {
            view_instance_id,
            version,
            payload,
        };

        let key = format!("{}.{}", A::aggregate_type(), &view.view_instance_id);
        debug!("update_view - {view:#?}");
        let view_data = serde_json::to_vec_pretty(&view)?;
        let mut buf = Cursor::new(view_data);
        let jetstream = async_nats::jetstream::new(self.client.clone());
        jetstream
            .get_object_store(&self.bucket_name)
            .await
            .map_err(|e| PersistenceError::ConnectionError(e.into()))?
            .put(key.as_str(), &mut buf)
            .await
            .map_err(|e| PersistenceError::ConnectionError(e.into()))?;
        Ok(())
    }
}
