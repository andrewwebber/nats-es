use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

use async_nats::ConnectOptions;
use async_trait::async_trait;
use cqrs_es::{
    persist::{GenericQuery, PersistedEventStore, ViewRepository},
    test::TestFramework,
    Aggregate, CqrsFramework, DomainEvent, Query, View,
};
use nats_es::{
    event_repository::{NatsEventStore, NatsEventStoreOptions},
    views_repository::NatsViewRepository,
};
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub enum BankAccountCommand {
    OpenAccount { account_id: String },
    DepositMoney { amount: f64 },
    WithdrawMoney { amount: f64 },
    WriteCheck { check_number: String, amount: f64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BankAccountEvent {
    AccountOpened {
        account_id: String,
    },
    CustomerDepositedMoney {
        amount: f64,
        balance: f64,
    },
    CustomerWithdrewCash {
        amount: f64,
        balance: f64,
    },
    CustomerWroteCheck {
        check_number: String,
        amount: f64,
        balance: f64,
    },
}

impl DomainEvent for BankAccountEvent {
    fn event_type(&self) -> String {
        let event_type: &str = match self {
            BankAccountEvent::AccountOpened { .. } => "AccountOpened",
            BankAccountEvent::CustomerDepositedMoney { .. } => "CustomerDepositedMoney",
            BankAccountEvent::CustomerWithdrewCash { .. } => "CustomerWithdrewCash",
            BankAccountEvent::CustomerWroteCheck { .. } => "CustomerWroteCheck",
        };
        event_type.to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug)]
pub struct BankAccountError(String);

impl Display for BankAccountError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for BankAccountError {}

impl From<&str> for BankAccountError {
    fn from(message: &str) -> Self {
        BankAccountError(message.to_string())
    }
}

pub struct BankAccountServices;

impl BankAccountServices {
    pub async fn atm_withdrawal(&self, _atm_id: &str, _amount: f64) -> Result<(), AtmError> {
        Ok(())
    }

    pub async fn validate_check(&self, _account: &str, _check: &str) -> Result<(), CheckingError> {
        Ok(())
    }
}
pub struct AtmError;
pub struct CheckingError;

#[derive(Serialize, Default, Deserialize)]
pub struct BankAccount {
    opened: bool,
    // this is a floating point for our example, don't do this IRL
    balance: f64,
}

#[async_trait]
impl Aggregate for BankAccount {
    type Command = BankAccountCommand;
    type Event = BankAccountEvent;
    type Error = BankAccountError;
    type Services = BankAccountServices;

    // This identifier should be unique to the system.
    fn aggregate_type() -> String {
        "account".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BankAccountCommand::DepositMoney { amount } => {
                let balance = self.balance + amount;
                Ok(vec![BankAccountEvent::CustomerDepositedMoney {
                    amount,
                    balance,
                }])
            }
            BankAccountCommand::WriteCheck {
                check_number,
                amount,
            } => {
                let balance = self.balance + amount;

                Ok(vec![BankAccountEvent::CustomerWroteCheck {
                    check_number,
                    amount,
                    balance,
                }])
            }
            _ => Ok(vec![]),
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::AccountOpened { .. } => self.opened = true,

            BankAccountEvent::CustomerDepositedMoney { amount: _, balance } => {
                self.balance = balance;
            }

            BankAccountEvent::CustomerWithdrewCash { amount: _, balance } => {
                self.balance = balance;
            }

            BankAccountEvent::CustomerWroteCheck {
                check_number: _,
                amount: _,
                balance,
            } => {
                self.balance = balance;
            }
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct BankView {
    pub balance: f64,
}

impl View<BankAccount> for BankView {
    fn update(&mut self, event: &cqrs_es::EventEnvelope<BankAccount>) {
        println!("updating bankview {:#?}", &event.payload);
        match &event.payload {
            BankAccountEvent::CustomerDepositedMoney { amount: _, balance } => {
                self.balance = *balance
            }
            BankAccountEvent::CustomerWithdrewCash { amount: _, balance } => {
                self.balance = *balance
            }
            BankAccountEvent::CustomerWroteCheck {
                check_number: _,
                amount: _,
                balance,
            } => self.balance = *balance,
            _ => (),
        }
    }
}

type AccountTestFramework = TestFramework<BankAccount>;

#[test]
fn test_deposit_money() {
    let expected = BankAccountEvent::CustomerDepositedMoney {
        amount: 200.0,
        balance: 200.0,
    };

    AccountTestFramework::with(BankAccountServices)
        .given_no_previous_events()
        .when(BankAccountCommand::DepositMoney { amount: 200.0 })
        .then_expect_events(vec![expected]);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub nats_connection: String,
    pub nats_user: String,
    pub nats_password: String,
    pub nats_domain: String,
    pub nats_replicas: usize,
}

#[tokio::test]
async fn test_event_store() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt::init();
    let config: Config =
        envy::from_env().expect("unable to load configuration from environment variables");
    let client = ConnectOptions::new()
        .user_and_password(config.nats_user.to_owned(), config.nats_password.to_owned())
        .connect(&config.nats_connection)
        .await
        .expect("unable to start nats client");

    let jetstream = async_nats::jetstream::with_domain(client.clone(), &config.nats_domain);
    jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "events_bank".to_string(),
            subjects: vec!["events.account.>".to_string()],
            num_replicas: config.nats_replicas,
            ..Default::default()
        })
        .await
        .unwrap();
    let repo = NatsEventStore::new(
        client.clone(),
        NatsEventStoreOptions {
            stream_name: "events_bank".to_string(),
            aggregate_types: vec![BankAccount::aggregate_type()],
            replicas: config.nats_replicas,
            domain: config.nats_domain.to_owned(),
        },
    )
    .await
    .unwrap();

    info!("creating kv store for views");
    jetstream
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "views_bank".to_string(),
            num_replicas: config.nats_replicas,
            ..Default::default()
        })
        .await
        .unwrap();

    let event_store = PersistedEventStore::new_event_store(repo);

    let bank_view_repo = Arc::new(NatsViewRepository::<BankView, BankAccount>::new(
        client.clone(),
        "views_bank",
        config.nats_domain.as_str(),
    ));
    let bank_view = GenericQuery::new(bank_view_repo.clone());

    let queries: Vec<Box<dyn Query<BankAccount>>> = vec![Box::new(bank_view)];
    let cqrs = CqrsFramework::new(event_store, queries, BankAccountServices {});

    info!("CQRS framework initialized");
    let aggregate_id = format!("account-{}", Uuid::new_v4());
    info!("aggregate id - {aggregate_id}");

    info!("deposit 1000");
    cqrs.execute(
        &aggregate_id,
        BankAccountCommand::DepositMoney { amount: 1000_f64 },
    )
    .await
    .unwrap();

    info!("checking balance");
    let view = bank_view_repo.load(&aggregate_id).await.unwrap().unwrap();
    println!("balance = {}", view.balance);

    info!("write a check for $236.15");
    cqrs.execute(
        &aggregate_id,
        BankAccountCommand::WriteCheck {
            check_number: "1337".to_string(),
            amount: 236.15,
        },
    )
    .await
    .unwrap();

    info!("checking balance");
    let view = bank_view_repo.load(&aggregate_id).await.unwrap().unwrap();
    println!("balance = {}", view.balance);
    assert_eq!(view.balance, 236.15 + 1000_f64);
}
