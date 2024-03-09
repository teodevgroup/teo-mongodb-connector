use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use async_trait::async_trait;
use bson::{doc, Document};
use mongodb::{Client, Collection, Database};
use mongodb::options::ClientOptions;
use teo_runtime::connection::connection::Connection;
use teo_runtime::connection::transaction::Transaction;
use crate::connector::OwnedSession;
use crate::connector::transaction::MongoDBTransaction;


#[derive(Debug)]
pub struct MongoDBConnection {
    client: Client,
    database: Database,
    supports_transaction: bool,
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
        let supports_transaction = Self::test_transaction_support(&client, &database).await;
        if !supports_transaction {
            println!("warning: MongoDB transaction is not supported in this setup.");
        }
        Self {
            client,
            database,
            supports_transaction,
        }
    }

    async fn test_transaction_support(client: &Client, database: &Database) -> bool {
        let Ok(mut session) = client.start_session(None).await else {
            return false;
        };
        let Ok(_) = session.start_transaction(None).await else {
            return false;
        };
        let collection: Collection<Document> = database.collection("__teo__transaction_test__");
        // match collection.insert_one_with_session(doc! {"supports": true}, None, &mut session).await {
        //     Ok(_) => (),
        //     Err(e) => println!("see this error: {:?}", e),
        // };
        let result = collection.insert_one_with_session(doc! {"supports": true}, None, &mut session).await.is_ok();
        let Ok(_) = session.commit_transaction().await else {
            return false;
        };
        result
    }
}

#[async_trait]
impl Connection for MongoDBConnection {

    async fn transaction(&self) -> teo_result::Result<Arc<dyn Transaction>> {
        if !self.supports_transaction {
            return self.no_transaction().await;
        }
        let session = OwnedSession::new(self.client.start_session(None).await.unwrap());
        session.start_transaction().await?;
        Ok(Arc::new(MongoDBTransaction {
            owned_session: Some(session),
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