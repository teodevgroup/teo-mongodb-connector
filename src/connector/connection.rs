use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use async_trait::async_trait;
use bson::doc;
use mongodb::{Client, Database};
use mongodb::options::ClientOptions;
use teo_runtime::connection::connection::Connection;
use teo_runtime::connection::transaction::Transaction;
use crate::connector::OwnedSession;
use crate::connector::transaction::MongoDBTransaction;


#[derive(Debug)]
pub struct MongoDBConnection {
    client: Client,
    database: Database,
}

impl MongoDBConnection {

    pub async fn new(url: &str) -> Self {
        let options = match ClientOptions::parse(url).await {
            Ok(options) => options,
            Err(_) => panic!("MongoDB url is invalid.")
        };
        let database_name = match &options.default_database {
            Some(database_name) => database_name,
            None => panic!("No database name found in MongoDB url.")
        };
        let client = match Client::with_options(options.clone()) {
            Ok(client) => client,
            Err(_) => panic!("MongoDB client creating error.")
        };
        match client.database("xxxxxpingpingpingxxxxx").run_command(doc! {"ping": 1}, None).await {
            Ok(_) => (),
            Err(_) => panic!("Cannot connect to MongoDB database."),
        }
        let database = client.database(&database_name);
        Self {
            client,
            database,
        }
    }
}

#[async_trait]
impl Connection for MongoDBConnection {

    async fn transaction(&self) -> teo_result::Result<Arc<dyn Transaction>> {
        Ok(Arc::new(MongoDBTransaction {
            owned_session: Some(OwnedSession::new(self.client.start_session(None).await.unwrap())),
            database: self.database.clone(),
            committed: Arc::new(AtomicBool::new(false)),
        }))
    }

    async fn no_transaction(&self) -> teo_result::Result<Arc<dyn Transaction>> {
        Ok(Arc::new(MongoDBTransaction {
            owned_session: None,
            database: self.database.clone(),
            committed: Arc::new(AtomicBool::new(false)),
        }))
    }
}