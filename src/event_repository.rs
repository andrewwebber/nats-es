use std::{collections::BTreeMap, time::Duration};

use async_nats::{
    jetstream::{
        consumer::pull,
        context::{CreateStreamError, Publish},
        message::StreamMessage,
        response::Response,
        stream::{Config, DirectGetErrorKind},
    },
    Client,
};
use async_trait::async_trait;
use cqrs_es::{
    persist::{
        PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent,
        SerializedSnapshot,
    },
    Aggregate,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

fn is_default<T: Default + Eq>(t: &T) -> bool {
    t == &T::default()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Info {
    #[serde(rename = "type")]
    ty: String,
    state: State,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    #[serde(default, skip_serializing_if = "is_default")]
    pub num_subjects: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub subjects: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsSerializedEvent {
    /// The id of the aggregate instance.
    pub aggregate_id: String,
    /// The sequence number of the event for this aggregate instance.
    pub sequence: usize,
    /// The type of aggregate the event applies to.
    pub aggregate_type: String,
    /// The type of event that is serialized.
    pub event_type: String,
    /// The version of event that is serialized.
    pub event_version: String,
    /// The serialized domain event.
    pub payload: Value,
    /// Additional metadata, serialized from a HashMap<String,String>.
    pub metadata: Value,
}

impl From<SerializedEvent> for NatsSerializedEvent {
    fn from(
        SerializedEvent {
            aggregate_id,
            sequence,
            aggregate_type,
            event_type,
            event_version,
            payload,
            metadata,
        }: SerializedEvent,
    ) -> Self {
        Self {
            aggregate_id,
            sequence,
            aggregate_type,
            event_type,
            event_version,
            payload,
            metadata,
        }
    }
}

pub struct NatsEventStoreOptions {
    pub stream_name: String,
    pub aggregate_types: Vec<String>,
    pub replicas: usize,
    pub domain: String,
}

pub struct NatsEventStore {
    client: Client,
    options: NatsEventStoreOptions,
}

impl NatsEventStore {
    pub async fn new(
        client: Client,
        options: NatsEventStoreOptions,
    ) -> Result<Self, CreateStreamError> {
        let jetstream = async_nats::jetstream::with_domain(client.clone(), &options.domain);
        jetstream
            .get_or_create_stream(Config {
                name: options.stream_name.to_owned(),
                subjects: options.aggregate_types.to_owned(),
                num_replicas: options.replicas,
                ..Default::default()
            })
            .await?;

        Ok(Self { client, options })
    }
}

#[async_trait]
impl PersistedEventRepository for NatsEventStore {
    /// Returns all events for a single aggregate instance.
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        info!("get events {aggregate_id}");

        let mut results = Vec::new();

        let jetstream =
            async_nats::jetstream::with_domain(self.client.clone(), &self.options.domain);

        let filter_subject = format!("events.{}.{aggregate_id}", A::aggregate_type());
        debug!("looking for subject '{filter_subject}'",);
        let response: Response<Info> = jetstream
            .request(
                format!("STREAM.INFO.{}", self.options.stream_name),
                &json!( {"offset":0,"subjects_filter":">"}),
            )
            .await
            .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;
        let info = match response {
            Response::Err { error } => Err(PersistenceError::ConnectionError(Box::new(error)))?,
            Response::Ok(r) => r,
        };

        let found = info.state.subjects.get(&filter_subject);
        let stream = jetstream
            .get_stream(&self.options.stream_name)
            .await
            .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

        if found.is_some() {
            let consumer = stream
                .create_consumer(pull::OrderedConfig {
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    replay_policy: async_nats::jetstream::consumer::ReplayPolicy::Instant,
                    filter_subject,
                    ..Default::default()
                })
                .await
                .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

            let mut pending = consumer.cached_info().num_pending;
            debug!("retrieving {pending} messages for '{aggregate_id}'");

            let mut messages = consumer
                .messages()
                .await
                .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

            debug!("looping over messages for '{aggregate_id}'");
            while let Some(Ok(message)) =
                tokio::time::timeout(Duration::from_secs(1), messages.next())
                    .await
                    .map_err(|e| {
                        warn!("{e}");
                        PersistenceError::OptimisticLockError
                    })?
            {
                let NatsSerializedEvent {
                    aggregate_id,
                    sequence,
                    aggregate_type,
                    event_type,
                    event_version,
                    payload,
                    metadata,
                } = serde_json::from_slice(&message.payload)?;
                let event = SerializedEvent::new(
                    aggregate_id,
                    sequence,
                    aggregate_type,
                    event_type,
                    event_version,
                    payload,
                    metadata,
                );
                results.push(event);

                message
                    .ack()
                    .await
                    .map_err(PersistenceError::UnknownError)?;
                pending -= 1;
                if pending == 0 {
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Returns the last events for a single aggregate instance.
    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        info!("get last events {aggregate_id}");
        let mut results = Vec::new();
        let jetstream =
            async_nats::jetstream::with_domain(self.client.clone(), &self.options.domain);

        let stream = jetstream
            .get_stream(&self.options.stream_name)
            .await
            .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

        let filter_subject = format!("events.{aggregate_id}");
        debug!("looking for subject '{filter_subject}'",);
        let found = match stream.direct_get_first_for_subject(&filter_subject).await {
            Ok(_) => true,
            Err(e) => match e.kind() {
                DirectGetErrorKind::NotFound | DirectGetErrorKind::InvalidSubject => false,
                _ => Err(PersistenceError::ConnectionError(Box::new(e)))?,
            },
        };

        if found {
            let consumer = stream
                .create_consumer(pull::OrderedConfig {
                    filter_subject,
                    ..Default::default()
                })
                .await
                .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

            let mut messages = consumer
                .messages()
                .await
                .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

            while let Some(Ok(message)) = messages.next().await {
                let NatsSerializedEvent {
                    aggregate_id,
                    sequence,
                    aggregate_type,
                    event_type,
                    event_version,
                    payload,
                    metadata,
                } = serde_json::from_slice(&message.payload)?;
                if sequence > last_sequence {
                    results.push(SerializedEvent::new(
                        aggregate_id,
                        sequence,
                        aggregate_type,
                        event_type,
                        event_version,
                        payload,
                        metadata,
                    ));
                }

                message
                    .ack()
                    .await
                    .map_err(PersistenceError::UnknownError)?;
            }
        }

        Ok(results)
    }

    /// Returns the current snapshot for an aggregate instance.
    async fn get_snapshot<A: Aggregate>(
        &self,
        _aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        debug!("get_snapshot");
        Ok(None)
    }

    /// Commits the updated aggregate and accompanying events.
    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        _snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        info!("persist events - {events:#?}");
        if events.is_empty() {
            return Ok(());
        }

        let jetstream =
            async_nats::jetstream::with_domain(self.client.clone(), &self.options.domain);

        let filter_subject = format!(
            "events.{}.{}",
            events[0].aggregate_type, events[0].aggregate_id
        );

        let response: Response<Info> = jetstream
            .request(
                format!("STREAM.INFO.{}", self.options.stream_name),
                &json!( {"offset":0,"subjects_filter":">"}),
            )
            .await
            .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

        let info = match response {
            Response::Err { error } => Err(PersistenceError::ConnectionError(Box::new(error)))?,
            Response::Ok(r) => r,
        };

        let mut expected_last_subject_sequence = None;
        let found = info.state.subjects.get(&filter_subject);
        if found.is_some() {
            let raw_message = jetstream
                .get_stream(&self.options.stream_name)
                .await
                .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?
                .get_last_raw_message_by_subject(&filter_subject)
                .await
                .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;
            expected_last_subject_sequence = Some(raw_message.sequence);

            let StreamMessage { headers, .. } = raw_message;

            match headers.get("event_sequence") {
                Some(event_sequence) => {
                    let sequence = events[0].sequence;
                    let event_sequence: usize = event_sequence
                        .to_string()
                        .parse()
                        .map_err(|_| PersistenceError::OptimisticLockError)?;

                    debug!("comparing last event sequence '{event_sequence} with '{sequence}'");
                    if event_sequence >= events[0].sequence {
                        warn!(
                            "found last event sequence '{event_sequence}' but it is newer than ours '{}'",
                            events[0].sequence
                        );
                    }
                }
                None => Err(PersistenceError::OptimisticLockError)?,
            };
        }

        for event in events.iter().cloned() {
            let event_sequence = event.sequence;
            let event_type = event.event_type.to_owned();

            let nats_event: NatsSerializedEvent = event.into();

            let nats_event = serde_json::to_vec_pretty(&nats_event)?;

            let mut publish = Publish::build()
                .payload(nats_event.into())
                .header("event_sequence", format!("{event_sequence}").as_str())
                .header("event_type", event_type.as_str());

            if let Some(s) = expected_last_subject_sequence {
                publish = publish.expected_last_subject_sequence(s);
            }

            let ack = jetstream
                .send_publish(filter_subject.to_owned(), publish)
                .await
                .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?
                .await
                .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;
            expected_last_subject_sequence = Some(ack.sequence);
        }

        Ok(())
    }

    /// Streams all events for an aggregate instance.
    async fn stream_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        info!("stream events {aggregate_id}");
        let (mut feed, st) = ReplayStream::new(1000);
        let jetstream =
            async_nats::jetstream::with_domain(self.client.clone(), &self.options.domain);

        let stream = jetstream
            .get_stream(&self.options.stream_name)
            .await
            .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

        let response: Response<Info> = jetstream
            .request(
                format!("STREAM.INFO.{}", self.options.stream_name),
                &json!( {"offset":0,"subjects_filter":">"}),
            )
            .await
            .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;
        let info = match response {
            Response::Err { error } => Err(PersistenceError::ConnectionError(Box::new(error)))?,
            Response::Ok(r) => r,
        };

        let filter_subject = format!("events.{}.{}", A::aggregate_type(), aggregate_id);
        let found = info.state.subjects.get(&filter_subject);
        if found.is_some() {
            let consumer = stream
                .get_or_create_consumer(
                    "pull",
                    pull::OrderedConfig {
                        filter_subject,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

            let mut messages = consumer
                .messages()
                .await
                .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

            tokio::spawn(async move {
                while let Some(message) = messages.next().await {
                    if let Ok(message) = message {
                        if let Ok(NatsSerializedEvent {
                            aggregate_id,
                            sequence,
                            aggregate_type,
                            event_type,
                            event_version,
                            payload,
                            metadata,
                        }) = serde_json::from_slice(&message.payload)
                        {
                            let e = SerializedEvent::new(
                                aggregate_id,
                                sequence,
                                aggregate_type,
                                event_type,
                                event_version,
                                payload,
                                metadata,
                            );

                            feed.push(Ok(e)).await.unwrap();
                            message.ack().await.unwrap();
                        }
                    }
                }
            });
        }

        Ok(st)
    }

    /// Streams all events for an aggregate type.
    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        info!("stream all events");
        let (mut feed, st) = ReplayStream::new(1000);
        let jetstream =
            async_nats::jetstream::with_domain(self.client.clone(), &self.options.domain);

        let stream = jetstream
            .get_stream(&self.options.stream_name)
            .await
            .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

        let consumer = stream
            .create_consumer(pull::OrderedConfig {
                replay_policy: async_nats::jetstream::consumer::ReplayPolicy::Instant,
                ..Default::default()
            })
            .await
            .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

        let mut pending = consumer.cached_info().num_pending;
        info!("retrieving {pending} messages ");

        if pending > 0 {
            let mut messages = consumer
                .messages()
                .await
                .map_err(|e| PersistenceError::ConnectionError(Box::new(e)))?;

            tokio::spawn(async move {
                while let Some(message) = messages.next().await {
                    match message {
                        Ok(message) => {
                            if let Ok(NatsSerializedEvent {
                                aggregate_id,
                                sequence,
                                aggregate_type,
                                event_type,
                                event_version,
                                payload,
                                metadata,
                            }) = serde_json::from_slice(&message.payload)
                            {
                                let e = SerializedEvent::new(
                                    aggregate_id,
                                    sequence,
                                    aggregate_type,
                                    event_type,
                                    event_version,
                                    payload,
                                    metadata,
                                );

                                feed.push(Ok(e)).await.unwrap();
                                message.ack().await.unwrap();
                            }
                        }
                        Err(e) => warn!("{e}"),
                    }

                    pending -= 1;
                    if pending == 0 {
                        break;
                    }
                }
            });
        }

        Ok(st)
    }
}

#[cfg(test)]
mod tests {
    use tracing::debug;

    use super::Info;

    #[test]
    fn test_deser_state() {
        let content = r#"{"type":"io.nats.jetstream.api.v1.stream_info_response","total":4,"offset":0,"limit":100000,"config":{"name":"events","subjects":["events.*"],"retention":"limits","max_consumers":-1,"max_msgs":-1,"max_bytes":-1,"max_age":0,"max_msgs_per_subject":-1,"max_msg_size":-1,"discard":"old","storage":"file","num_replicas":1,"duplicate_window":120000000000,"compression":"none","allow_direct":true,"mirror_direct":false,"sealed":false,"deny_delete":false,"deny_purge":false,"allow_rollup_hdrs":false,"consumer_limits":{}},"created":"2024-03-12T07:22:27.726983206Z","state":{"messages":20,"bytes":6032,"first_seq":1,"first_ts":"2024-03-12T07:40:50.421552408Z","last_seq":20,"last_ts":"2024-03-12T07:45:25.551797327Z","num_subjects":4,"subjects":{"events.31525":5,"events.31606":5,"events.32460":5,"events.33735":5},"consumer_count":0},"cluster":{"name":"test-cluster","leader":"test-cluster3"},"ts":"2024-03-12T07:59:38.054607389Z"}"#;
        let info: Info = serde_json::from_str(content).unwrap();
        debug!("{info:#?}");

        let content ="{\"type\":\"io.nats.jetstream.api.v1.stream_info_response\",\"total\":0,\"offset\":0,\"limit\":0,\"config\":{\"name\":\"events\",\"subjects\":[\"events.*\"],\"retention\":\"limits\",\"max_consumers\":-1,\"max_msgs\":-1,\"max_bytes\":-1,\"max_age\":0,\"max_msgs_per_subject\":-1,\"max_msg_size\":-1,\"discard\":\"old\",\"storage\":\"file\",\"num_replicas\":1,\"duplicate_window\":120000000000,\"compression\":\"none\",\"allow_direct\":true,\"mirror_direct\":false,\"sealed\":false,\"deny_delete\":false,\"deny_purge\":false,\"allow_rollup_hdrs\":false,\"consumer_limits\":{}},\"created\":\"2024-03-12T09:24:34.51844732Z\",\"state\":{\"messages\":0,\"bytes\":0,\"first_seq\":0,\"first_ts\":\"0001-01-01T00:00:00Z\",\"last_seq\":0,\"last_ts\":\"0001-01-01T00:00:00Z\",\"consumer_count\":0},\"cluster\":{\"name\":\"test-cluster\",\"leader\":\"test-cluster1\"},\"ts\":\"2024-03-12T09:25:53.405004504Z\"}";
        let info: Info = serde_json::from_str(content).unwrap();
        debug!("{info:#?}");
    }
}
