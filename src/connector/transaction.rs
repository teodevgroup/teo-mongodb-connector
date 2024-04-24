use std::fmt::{Debug};
use std::ops::Neg;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use async_trait::async_trait;
use bson::{Bson, doc, Document};
use futures_util::StreamExt;
use key_path::{KeyPath, path};
use mongodb::{Database, Collection, IndexModel, ClientSession};
use mongodb::error::{ErrorKind, WriteFailure, Error as MongoDBError};
use mongodb::options::{FindOneAndUpdateOptions, IndexOptions, ReturnDocument};
use regex::Regex;
use crate::aggregation::Aggregation;
use crate::bson_ext::coder::BsonCoder;
use teo_runtime::action::action::*;
use teo_runtime::model::object::Object;
use teo_runtime::index::Type;
use teo_runtime::model::{Index, Model};
use teo_runtime::value::Value;
use teo_result::{Error, Result};
use teo_runtime::connection::transaction::{Ctx, Transaction};
use teo_runtime::model::field::column_named::ColumnNamed;
use teo_runtime::model::field::is_optional::IsOptional;
use teo_runtime::traits::named::Named;
use teo_runtime::model::field::typed::Typed;
use teo_runtime::sort::Sort;
use teo_runtime::model::object::input::Input;
use teo_runtime::namespace::Namespace;
use teo_runtime::error_ext;
use teo_runtime::utils::ContainsStr;
use teo_runtime::teon;
use crate::bson_ext::teon_value_to_bson;
use crate::connector::OwnedSession;
use crate::migration::index_model::FromIndexModel;

#[derive(Debug, Clone)]
pub struct MongoDBTransaction {
    pub(super) database: Database,
    pub(super) owned_session: Option<OwnedSession>,
    pub committed: Arc<AtomicBool>,
}

impl MongoDBTransaction {

    pub(crate) fn session(&self) -> Option<&mut ClientSession> {
        if self.committed.load(Ordering::SeqCst) {
            None
        } else {
            match &self.owned_session {
                None => None,
                Some(s) => Some(s.client_session()),
            }
        }
    }

    pub(crate) fn get_collection(&self, model: &Model) -> Collection<Document> {
        self.database.collection(model.table_name())
    }

    fn document_to_object(&self, transaction_ctx: Ctx, document: &Document, object: &Object, select: Option<&Value>, include: Option<&Value>) -> Result<()> {
        for key in document.keys() {
            let object_field = object.model().fields().iter().find(|f| f.column_name() == key).map(|f| *f);
            if object_field.is_some() {
                // field
                let object_field = object_field.unwrap();
                let object_key = &object_field.name;
                let r#type = object_field.r#type();
                let bson_value = document.get(key).unwrap();
                let value_result = BsonCoder::decode(transaction_ctx.namespace(), object.model(), r#type, object_field.is_optional(), bson_value, path![]);
                match value_result {
                    Ok(value) => {
                        object.set_value(object_key, value).unwrap();
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
            } else {
                // relation
                let relation = object.model().relation(key);
                if relation.is_none() {
                    continue;
                }
                let inner_finder = if let Some(include) = include {
                    include.get(key)
                } else {
                    None
                };
                let inner_select = if let Some(inner_finder) = inner_finder {
                    inner_finder.get("select")
                } else {
                    None
                };
                let inner_include = if let Some(inner_finder) = inner_finder {
                    inner_finder.get("include")
                } else {
                    None
                };
                let relation = relation.unwrap();
                let relation_model = transaction_ctx.namespace().model_at_path(&relation.model_path()).unwrap();
                let object_bsons = document.get(key).unwrap().as_array().unwrap();
                let mut related: Vec<Object> = vec![];
                for related_object_bson in object_bsons {
                    let action = NESTED | FIND | (if relation.is_vec { MANY } else { SINGLE });
                    let related_object = transaction_ctx.new_object(relation_model, action, object.request_ctx())?;
                    self.clone().document_to_object(transaction_ctx.clone(), related_object_bson.as_document().unwrap(), &related_object, inner_select, inner_include)?;
                    related.push(related_object);
                }
                object.inner.relation_query_map.lock().unwrap().insert(key.to_string(), related);
            }
        }
        object.inner.is_initialized.store(true, Ordering::SeqCst);
        object.inner.is_new.store(false, Ordering::SeqCst);
        object.set_select(select).unwrap();
        Ok(())
    }

    fn _handle_write_error(&self, error_kind: &ErrorKind, object: &Object, path: KeyPath) -> Error {
        return match error_kind {
            ErrorKind::Write(write) => {
                match write {
                    WriteFailure::WriteError(write_error) => {
                        match write_error.code {
                            11000 => {
                                let full_regex = Regex::new(r"dup key: (.+)").unwrap();
                                let regex = Regex::new(r"dup key: \{ (.+?):").unwrap();
                                let full_message = full_regex.captures(write_error.message.as_str()).unwrap().get(1).unwrap().as_str();
                                let field_column_name = regex.captures(write_error.message.as_str()).unwrap().get(1).unwrap().as_str();
                                if let Some(field_column) = object.model().field_with_column_name(field_column_name) {
                                    error_ext::unique_value_duplicated(path + field_column.name(), full_message)
                                } else {
                                    error_ext::unique_value_duplicated(path, full_message)
                                }
                            }
                            _ => {
                                error_ext::unknown_database_write_error(path, write_error.message.as_str())
                            }
                        }
                    }
                    WriteFailure::WriteConcernError(write_concern) => {
                        error_ext::unknown_database_write_error(path, write_concern.message.as_str())
                    }
                    _ => {
                        error_ext::unknown_database_write_error(path, "unknown write failure")
                    }
                }
            }
            ErrorKind::Transaction { message, .. } => {
                error_ext::unknown_database_write_error(path, message.as_str())
            }
            ErrorKind::SessionsNotSupported => {
                error_ext::unknown_database_write_error(path, "session is not supported")
            }
            _ => {
                error_ext::unknown_database_write_error(path, format!("unknown write: {:?}", error_kind))
            }
        }
    }

    async fn aggregate_to_documents(&self, aggregate_input: Vec<Document>, col: Collection<Document>, path: KeyPath) -> Result<Vec<std::result::Result<Document, MongoDBError>>> {
        match self.session() {
            Some(session) => {
                let cur = col.aggregate_with_session(aggregate_input, None, session).await;
                if cur.is_err() {
                    return Err(error_ext::unknown_database_find_error(path, format!("{:?}", cur)));
                }
                let mut cur = cur.unwrap();
                let mut results: Vec<std::result::Result<Document, MongoDBError>> = vec![];
                loop {
                    if let Some(item) = cur.next(session).await {
                        results.push(item);
                    } else {
                        break;
                    }
                }
                Ok(results)
            },
            None => {
                let cur = col.aggregate(aggregate_input, None).await;
                if cur.is_err() {
                    return Err(error_ext::unknown_database_find_error(path, format!("{:?}", cur)));
                }
                let cur = cur.unwrap();
                let results: Vec<std::result::Result<Document, MongoDBError>> = cur.collect().await;
                Ok(results)
            },
        }
    }

    async fn aggregate_or_group_by(&self, namespace: &Namespace, model: &Model, finder: &Value, path: KeyPath) -> Result<Vec<Value>> {
        let aggregate_input = Aggregation::build_for_aggregate(namespace, model, finder)?;
        let col = self.get_collection(model);
        let results = self.aggregate_to_documents(aggregate_input, col, path).await?;
        let mut final_retval: Vec<Value> = vec![];
        for result in results.iter() {
            // there are records
            let data = result.as_ref().unwrap();
            let mut retval = teon!({});
            for (g, o) in data {
                if g.as_str() == "_id" {
                    continue;
                }
                // aggregate
                if g.starts_with("_") {
                    retval.as_dictionary_mut().unwrap().insert(g.clone(), teon!({}));
                    for (dbk, v) in o.as_document().unwrap() {
                        let k = dbk;
                        if let Some(f) = v.as_f64() {
                            retval.as_dictionary_mut().unwrap().get_mut(g.as_str()).unwrap().as_dictionary_mut().unwrap().insert(k.to_string(), teon!(f));
                        } else if let Some(i) = v.as_i64() {
                            retval.as_dictionary_mut().unwrap().get_mut(g.as_str()).unwrap().as_dictionary_mut().unwrap().insert(k.to_string(), teon!(i));
                        } else if let Some(i) = v.as_i32() {
                            retval.as_dictionary_mut().unwrap().get_mut(g.as_str()).unwrap().as_dictionary_mut().unwrap().insert(k.to_string(), teon!(i));
                        } else if v.as_null().is_some() {
                            retval.as_dictionary_mut().unwrap().get_mut(g.as_str()).unwrap().as_dictionary_mut().unwrap().insert(k.to_string(), teon!(null));
                        }
                    }
                } else {
                    // group by field
                    let field = model.field(g).unwrap();
                    let val = if o.as_null().is_some() { Value::Null } else {
                        BsonCoder::decode(namespace, model, field.r#type(), true, o, path![])?
                    };
                    let json_val = val;
                    retval.as_dictionary_mut().unwrap().insert(g.to_string(), json_val);
                }
            }
            final_retval.push(retval);
        }
        Ok(final_retval)
    }

    async fn create_object(&self, object: &Object, path: KeyPath) -> Result<()> {
        let namespace = object.namespace();
        let model = object.model();
        let keys = object.keys_for_save();
        let col = self.get_collection(model);
        let auto_keys = &model.cache.auto_keys;
        // create
        let mut doc = doc!{};
        for key in keys {
            if let Some(field) = model.field(key) {
                let column_name = field.column_name();
                let val: Bson = BsonCoder::encode(field.r#type(), object.get_value(&key).unwrap())?;
                if val != Bson::Null {
                    doc.insert(column_name, val);
                }
            } else if let Some(property) = model.property(key) {
                let val: Bson = BsonCoder::encode(property.r#type(), object.get_property_value(&key).await?)?;
                if val != Bson::Null {
                    doc.insert(key, val);
                }
            }
        }
        let result = match self.session() {
            Some(session) => {
                col.insert_one_with_session(doc, None, session).await
            }
            None => {
                col.insert_one(doc, None).await
            }
        };
        match result {
            Ok(insert_one_result) => {
                let id = insert_one_result.inserted_id;
                for key in auto_keys {
                    let field = model.field(key).unwrap();
                    if field.column_name() == "_id" {
                        let new_value = BsonCoder::decode(namespace, model, field.r#type(), field.is_optional(), &id, path![]).unwrap();
                        object.set_value(field.name(), new_value)?;
                    }
                }
            }
            Err(error) => {
                return Err(self._handle_write_error(&error.kind, object, path));
            }
        }
        Ok(())
    }

    async fn update_object(&self, object: &Object, path: KeyPath) -> Result<()> {
        let namespace = object.namespace();
        let model = object.model();
        let keys = object.keys_for_save();
        let col = self.get_collection(model);
        let identifier: Bson = teon_value_to_bson(&object.db_identifier());
        let identifier = identifier.as_document().unwrap();
        let mut set = doc!{};
        let mut unset = doc!{};
        let mut inc = doc!{};
        let mut mul = doc!{};
        let mut push = doc!{};
        for key in keys {
            if let Some(field) = model.field(key) {
                let column_name = field.column_name();
                if let Some(updator) = object.get_atomic_updator(key) {
                    let (key, val) = Input::key_value(updator.as_dictionary().unwrap());
                    match key {
                        "increment" => inc.insert(column_name, teon_value_to_bson(val)),
                        "decrement" => inc.insert(column_name, teon_value_to_bson(&val.neg().unwrap())),
                        "multiply" => mul.insert(column_name, teon_value_to_bson(val)),
                        "divide" => mul.insert(column_name, Bson::Double(val.recip().unwrap().to_float().unwrap().abs())),
                        "push" => push.insert(column_name, teon_value_to_bson(val)),
                        _ => panic!("Unhandled key."),
                    };
                } else {
                    let bson_val: Bson = BsonCoder::encode(field.r#type(), object.get_value(&key).unwrap())?;
                    if bson_val == Bson::Null {
                        unset.insert(key, bson_val);
                    } else {
                        set.insert(key, bson_val);
                    }
                }
            } else if let Some(property) = model.property(key) {
                let bson_val: Bson = BsonCoder::encode(property.r#type(), object.get_property_value(&key).await?)?;
                if bson_val != Bson::Null {
                    set.insert(key, bson_val);
                } else {
                    unset.insert(key, bson_val);
                }
            }
        }
        let mut update_doc = doc!{};
        let mut return_new = false;
        if !set.is_empty() {
            update_doc.insert("$set", set);
        }
        if !unset.is_empty() {
            update_doc.insert("$unset", unset);
        }
        if !inc.is_empty() {
            update_doc.insert("$inc", inc);
            return_new = true;
        }
        if !mul.is_empty() {
            update_doc.insert("$mul", mul);
            return_new = true;
        }
        if !push.is_empty() {
            update_doc.insert("$push", push);
            return_new = true;
        }
        if update_doc.is_empty() {
            return Ok(());
        }
        if !return_new {
            let result = match self.session() {
                None => col.update_one(identifier.clone(), update_doc, None).await,
                Some(session) => col.update_one_with_session(identifier.clone(), update_doc, None, session).await,
            };
            return match result {
                Ok(_) => Ok(()),
                Err(error) => {
                    Err(self._handle_write_error(&error.kind, object, path))
                }
            }
        } else {
            let options = FindOneAndUpdateOptions::builder().return_document(ReturnDocument::After).build();
            let result = match self.session() {
                None => col.find_one_and_update(identifier.clone(), update_doc, options).await,
                Some(session) => col.find_one_and_update_with_session(identifier.clone(), update_doc, options, session).await,
            };
            match result {
                Ok(updated_document) => {
                    for (key, value) in object.inner.atomic_updater_map.lock().unwrap().iter() {
                        let bson_new_val = updated_document.as_ref().unwrap().get(key).unwrap();
                        let field = object.model().field(key).unwrap();
                        let field_value = BsonCoder::decode(namespace, model, field.r#type(), field.is_optional(), bson_new_val, path![])?;
                        object.set_value(key, field_value).unwrap();
                    }
                }
                Err(error) => {
                    return Err(self._handle_write_error(&error.kind, object, path));
                }
            }
        }
        Ok(())
    }

}

#[async_trait]
impl Transaction for MongoDBTransaction {

    async fn migrate(&self, models: Vec<&Model>, dry_run: bool, reset_database: bool, silent: bool) -> Result<()> {
        if reset_database {
            let _ = self.database.drop(None).await;
        }
        for model in models {
            let _name = model.name();
            let collection = self.get_collection(model);
            let mut reviewed_names: Vec<String> = Vec::new();
            let cursor_result = collection.list_indexes(None).await;
            if cursor_result.is_ok() {
                let mut cursor = cursor_result.unwrap();
                while let Some(Ok(index)) = cursor.next().await {
                    if index.keys == doc!{"_id": 1} {
                        continue
                    }
                    let name = (&index).options.as_ref().unwrap().name.as_ref().unwrap();
                    let result = model.indexes().iter().find(|i| name == i.name()).map(|i| *i);
                    if result.is_none() {
                        // not in our model definition, but in the database
                        // drop this index
                        let _ = collection.drop_index(name, None).await.unwrap();
                    } else {
                        let result = result.unwrap();
                        let our_format_index: Index = Index::from_index_model(&index);
                        if result != &our_format_index {
                            // alter this index
                            // drop first
                            let _ = collection.drop_index(name, None).await.unwrap();
                            // create index
                            let index_options = IndexOptions::builder()
                                .name(result.name().to_string())
                                .unique(result.r#type() == Type::Unique || result.r#type() == Type::Primary)
                                .sparse(true)
                                .build();
                            let mut keys = doc!{};
                            for item in result.items() {
                                let field = model.field(&item.field).unwrap();
                                let column_name = field.column_name();
                                keys.insert(column_name, if item.sort == Sort::Asc { 1 } else { -1 });
                            }
                            let index_model = IndexModel::builder().keys(keys).options(index_options).build();
                            let _result = collection.create_index(index_model, None).await;
                        }
                    }
                    reviewed_names.push(name.clone());
                }
            }
            for index in model.indexes() {
                if !reviewed_names.contains_str(index.name()) {
                    // ignore primary
                    if index.keys().len() == 1 {
                        let field = model.field(index.keys().get(0).unwrap()).unwrap();
                        if field.column_name() == "_id" {
                            continue
                        }
                    }
                    // create this index
                    let index_options = IndexOptions::builder()
                        .name(index.name().to_string())
                        .unique(index.r#type() == Type::Unique || index.r#type() == Type::Primary)
                        .sparse(true)
                        .build();
                    let mut keys = doc!{};
                    for item in index.items() {
                        let field = model.field(&item.field).unwrap();
                        let column_name = field.column_name();
                        keys.insert(column_name, if item.sort == Sort::Asc { 1 } else { -1 });
                    }
                    let index_model = IndexModel::builder().keys(keys).options(index_options).build();
                    let result = collection.create_index(index_model, None).await;
                    if result.is_err() {
                        println!("index create error: {:?}", result.err().unwrap());
                    }
                }
            }
        }
        Ok(())
    }

    async fn purge(&self, models: Vec<&Model>) -> Result<()> {
        for model in models {
            let col = self.get_collection(model);
            col.drop(None).await.unwrap();
        }
        Ok(())
    }

    async fn query_raw(&self, value: &Value) -> Result<Value> {
        unreachable!()
    }

    async fn save_object(&self, object: &Object, path: KeyPath) -> Result<()> {
        if object.is_new() {
            self.create_object(object, path).await
        } else {
            self.update_object(object, path).await
        }
    }

    async fn delete_object(&self, object: &Object, path: KeyPath) -> Result<()> {
        if object.is_new() {
            return Err(error_ext::object_is_not_saved_thus_cant_be_deleted(path));
        }
        let model = object.model();
        let col = self.get_collection(model);
        let bson_identifier: Bson = teon_value_to_bson(&object.db_identifier());
        let document_identifier = bson_identifier.as_document().unwrap();
        let result = match self.session() {
            None => col.delete_one(document_identifier.clone(), None).await,
            Some(session) => col.delete_one_with_session(document_identifier.clone(), None, session).await,
        };
        return match result {
            Ok(_result) => Ok(()),
            Err(err) => {
                Err(error_ext::unknown_database_delete_error(path, format!("{}", err)))
            }
        }
    }

    async fn find_unique(&self, model: &'static Model, finder: &Value, ignore_select_and_include: bool, action: Action, transaction_ctx: Ctx, req_ctx: Option<teo_runtime::request::Ctx>, path: KeyPath) -> Result<Option<Object>> {
        let select = finder.get("select");
        let include = finder.get("include");
        let aggregate_input = Aggregation::build(transaction_ctx.namespace(), model, finder)?;
        let col = self.get_collection(model);
        let results = self.aggregate_to_documents(aggregate_input, col, path).await?;
        if results.is_empty() {
            Ok(None)
        } else {
            for doc in results {
                let obj = transaction_ctx.new_object(model, action, req_ctx)?;
                self.clone().document_to_object(transaction_ctx, &doc.unwrap(), &obj, select, include)?;
                return Ok(Some(obj));
            }
            Ok(None)
        }
    }

    async fn find_many(&self, model: &'static Model, finder: &Value, ignore_select_and_include: bool, action: Action, transaction_ctx: Ctx, req_ctx: Option<teo_runtime::request::Ctx>, path: KeyPath) -> Result<Vec<Object>> {
        let select = finder.get("select");
        let include = finder.get("include");
        let aggregate_input = Aggregation::build(transaction_ctx.namespace(), model, finder)?;
        let reverse = Input::has_negative_take(finder);
        let col = self.get_collection(model);
        // println!("see aggregate input: {:?}", aggregate_input);
        let mut result = vec![];
        let results: Vec<std::result::Result<Document, MongoDBError>> = self.aggregate_to_documents(aggregate_input, col, path.clone()).await?;
        for doc in results {
            let obj = transaction_ctx.new_object(model, action, req_ctx.clone())?;
            match self.clone().document_to_object(transaction_ctx.clone(), &doc.unwrap(), &obj, select, include) {
                Ok(_) => {
                    if reverse {
                        result.insert(0, obj);
                    } else {
                        result.push(obj);
                    }
                }
                Err(err) => {
                    return Err(error_ext::unknown_database_find_error(path, format!("{}", err)));
                }
            }
        }
        Ok(result)
    }

    async fn count(&self, model: &'static Model, finder: &Value, transaction_ctx: Ctx, path: KeyPath) -> Result<Value> {
        if finder.get("select").is_some() {
            self.count_fields(model, finder, transaction_ctx, path).await
        } else {
            let counts = self.count_objects(model, finder, transaction_ctx, path).await?;
            Ok(Value::Int64(counts as i64))
        }
    }

    async fn count_objects(&self, model: &'static Model, finder: &Value, transaction_ctx: Ctx, path: KeyPath) -> Result<usize> {
        let input = Aggregation::build_for_count(transaction_ctx.namespace(), model, finder)?;
        let col = self.get_collection(model);
        let results = self.aggregate_to_documents(input, col, path).await?;
        if results.is_empty() {
            Ok(0)
        } else {
            let v = results.get(0).unwrap().as_ref().unwrap();
            let bson_count = v.get("count").unwrap();
            match bson_count {
                Bson::Int32(i) => Ok(*i as usize),
                Bson::Int64(i) => Ok(*i as usize),
                _ => panic!("Unhandled count number type.")
            }
        }
    }

    async fn count_fields(&self, model: &'static Model, finder: &Value, transaction_ctx: Ctx, path: KeyPath) -> Result<Value> {
        let new_finder = Value::Dictionary(finder.as_dictionary().unwrap().iter().map(|(k, v)| {
            if k.as_str() == "select" {
                ("_count".to_owned(), v.clone())
            } else {
                (k.to_owned(), v.clone())
            }
        }).collect());
        let aggregate_value = self.aggregate(model, &new_finder, transaction_ctx, path).await?;
        Ok(aggregate_value.get("_count").unwrap().clone())
    }

    async fn aggregate(&self, model: &'static Model, finder: &Value, transaction_ctx: Ctx, path: KeyPath) -> Result<Value> {
        let results = self.aggregate_or_group_by(transaction_ctx.namespace(), model, finder, path).await?;
        if results.is_empty() {
            // there is no record
            let mut retval = teon!({});
            for (g, o) in finder.as_dictionary().unwrap() {
                retval.as_dictionary_mut().unwrap().insert(g.clone(), teon!({}));
                for (k, _v) in o.as_dictionary().unwrap() {
                    let value = if g == "_count" { teon!(0) } else { teon!(null) };
                    retval.as_dictionary_mut().unwrap().get_mut(g.as_str()).unwrap().as_dictionary_mut().unwrap().insert(k.to_string(), value);
                }
            }
            Ok(retval)
        } else {
            Ok(results.get(0).unwrap().clone())
        }
    }

    async fn group_by(&self, model: &'static Model, finder: &Value, transaction_ctx: Ctx, path: KeyPath) -> Result<Vec<Value>> {
        Ok(self.aggregate_or_group_by(transaction_ctx.namespace(), model, finder, path).await?)
    }

    async fn sql(&self, model: &'static Model, sql: &str, transaction_ctx: Ctx) -> Result<Vec<Value>> {
        Err(Error::new("do not run raw sql on MongoDB database"))
    }

    fn is_committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }

    fn is_transaction(&self) -> bool {
        self.owned_session.is_some()
    }

    async fn commit(&self) -> Result<()> {
        if let Some(session) = &self.owned_session {
            session.commit_transaction().await
        } else {
            Ok(())
        }
    }

    async fn abort(&self) -> Result<()> {
        if let Some(session) = &self.owned_session {
            session.abort_transaction().await
        } else {
            Ok(())
        }
    }

    async fn spawn(&self) -> Result<Arc<dyn Transaction>> {
        Ok(Arc::new(self.clone()))
    }
}

unsafe impl Sync for MongoDBTransaction {}
unsafe impl Send for MongoDBTransaction {}
