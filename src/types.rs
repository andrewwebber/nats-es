use cqrs_es::{persist::PersistedEventStore, CqrsFramework};

use super::event_repository::NatsEventStore;

pub type NatsCqrs<A> = CqrsFramework<A, PersistedEventStore<NatsEventStore, A>>;
