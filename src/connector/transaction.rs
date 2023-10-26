use std::fmt::{Debug};
use std::ops::Neg;
use std::sync::Arc;
use std::sync::atomic::{Ordering};
use async_trait::async_trait;
use bson::{Bson, doc, Document};
use futures_util::StreamExt;
use key_path::{KeyPath, path};
use mongodb::{Database, Collection, IndexModel};
use mongodb::error::{ErrorKind, WriteFailure, Error as MongoDBError};
use mongodb::options::{FindOneAndUpdateOptions, IndexOptions, ReturnDocument};
use regex::Regex;
use crate::aggregation::Aggregation;
use crate::bson_ext::coder::BsonCoder;
use teo_runtime::action::*;
use teo_runtime::model::object::Object;
use teo_runtime::index::Type;
use teo_runtime::model::{Index, Model};
use teo_teon::value::Value;
use teo_result::{Error, Result};
use teo_runtime::connection::transaction::{Ctx, Transaction};
use teo_runtime::model::field::column_named::ColumnNamed;
use teo_runtime::model::field::named::Named;
use teo_runtime::sort::Sort;
use teo_runtime::model::object::input::Input;
use teo_runtime::namespace::Namespace;
use teo_runtime::object::error_ext;
use teo_teon::teon;
use crate::bson_ext::teon_value_to_bson;

#[derive(Debug, Clone)]
pub(crate) struct MongoDBTransaction {
    pub(super) database: Database,
}

impl MongoDBTransaction {

    pub(crate) fn get_collection(&self, model: &Model) -> Collection<Document> {
        self.database.collection(model.table_name())
    }

    fn document_to_object(&self, document: &Document, object: &Object, select: Option<&Value>, include: Option<&Value>) -> Result<()> {
        for key in document.keys() {
            let object_field = object.model().fields().iter().find(|f| f.column_name() == key);
            if object_field.is_some() {
                // field
                let object_field = object_field.unwrap();
                let object_key = &object_field.name;
                let field_type = object_field.field_type();
                let bson_value = document.get(key).unwrap();
                let value_result = BsonCoder::decode(object.model(), field_type, object_field.is_optional(), bson_value, path![]);
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
                let model_name = relation.model();
                let object_bsons = document.get(key).unwrap().as_array().unwrap();
                let mut related: Vec<Object> = vec![];
                for related_object_bson in object_bsons {
                    let action = NESTED | FIND | (if relation.is_vec { MANY } else { SINGLE });
                    let related_object = object.graph().new_object(model_name, action, object.action_source().clone(), Arc::new(self.clone()))?;
                    self.clone().document_to_object(related_object_bson.as_document().unwrap(), &related_object, inner_select, inner_include)?;
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

    fn _handle_write_error(&self, error_kind: &ErrorKind, object: &Object) -> Error {
        return match error_kind {
            ErrorKind::Write(write) => {
                match write {
                    WriteFailure::WriteError(write_error) => {
                        match write_error.code {
                            11000 => {
                                let regex = Regex::new(r"dup key: \{ (.+?):").unwrap();
                                let field_column_name = regex.captures(write_error.message.as_str()).unwrap().get(1).unwrap().as_str();
                                let field_name = object.model().field_with_column_name(field_column_name).unwrap().name();
                                Error::unique_value_duplicated(field_name.to_string())
                            }
                            _ => {
                                Error::unknown_database_write_error()
                            }
                        }
                    }
                    _ => {
                        Error::unknown_database_write_error()
                    }
                }
            }
            _ => {
                Error::unknown_database_write_error()
            }
        }
    }

    async fn aggregate_or_group_by(&self, namespace: &Namespace, model: &Model, finder: &Value) -> Result<Vec<Value>> {
        let aggregate_input = Aggregation::build_for_aggregate(namespace, model, finder)?;
        let col = self.get_collection(model);
        let cur = col.aggregate(aggregate_input, None).await;
        if cur.is_err() {
            println!("{:?}", cur);
            return Err(Error::unknown_database_find_error());
        }
        let cur = cur.unwrap();
        let results: Vec<std::result::Result<Document, MongoDBError>> = cur.collect().await;
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
                    retval.as_hashmap_mut().unwrap().insert(g.clone(), teon!({}));
                    for (dbk, v) in o.as_document().unwrap() {
                        let k = dbk;
                        if let Some(f) = v.as_f64() {
                            retval.as_hashmap_mut().unwrap().get_mut(g.as_str()).unwrap().as_hashmap_mut().unwrap().insert(k.to_string(), teon!(f));
                        } else if let Some(i) = v.as_i64() {
                            retval.as_hashmap_mut().unwrap().get_mut(g.as_str()).unwrap().as_hashmap_mut().unwrap().insert(k.to_string(), teon!(i));
                        } else if let Some(i) = v.as_i32() {
                            retval.as_hashmap_mut().unwrap().get_mut(g.as_str()).unwrap().as_hashmap_mut().unwrap().insert(k.to_string(), teon!(i));
                        } else if v.as_null().is_some() {
                            retval.as_hashmap_mut().unwrap().get_mut(g.as_str()).unwrap().as_hashmap_mut().unwrap().insert(k.to_string(), teon!(null));
                        }
                    }
                } else {
                    // group by field
                    let field = model.field(g).unwrap();
                    let val = if o.as_null().is_some() { Value::Null } else {
                        BsonCoder::decode(model, field.field_type(), true, o, path![])?
                    };
                    let json_val = val;
                    retval.as_hashmap_mut().unwrap().insert(g.to_string(), json_val);
                }
            }
            final_retval.push(retval);
        }
        Ok(final_retval)
    }

    async fn create_object(&self, object: &Object) -> teo_runtime::path::Result<()> {
        let model = object.model();
        let keys = object.keys_for_save();
        let col = self.get_collection(model);
        let auto_keys = model.auto_keys();
        // create
        let mut doc = doc!{};
        for key in keys {
            if let Some(field) = model.field(key) {
                let column_name = field.column_name();
                let val: Bson = BsonCoder::encode(field.field_type(), object.get_value(&key).unwrap())?;
                if val != Bson::Null {
                    doc.insert(column_name, val);
                }
            } else if let Some(property) = model.property(key) {
                let val: Bson = BsonCoder::encode(property.field_type(), object.get_property(&key).await.unwrap())?;
                if val != Bson::Null {
                    doc.insert(key, val);
                }
            }
        }
        let result = col.insert_one(doc, None).await;
        match result {
            Ok(insert_one_result) => {
                let id = insert_one_result.inserted_id;
                for key in auto_keys {
                    let field = model.field(key).unwrap();
                    if field.column_name() == "_id" {
                        let new_value = BsonCoder::decode(model, field.field_type(), field.is_optional(), &id, path![]).unwrap();
                        object.set_value(field.name(), new_value)?;
                    }
                }
            }
            Err(error) => {
                return Err(self._handle_write_error(&error.kind, object));
            }
        }
        Ok(())
    }

    async fn update_object(&self, object: &Object) -> teo_runtime::path::Result<()> {
        let model = object.model();
        let keys = object.keys_for_save();
        let col = self.get_collection(model);
        let identifier: Bson = teon_value_to_bson(object.db_identifier());
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
                    let (key, val) = Input::key_value(updator.as_hashmap().unwrap());
                    match key {
                        "increment" => inc.insert(column_name, teon_value_to_bson(val)),
                        "decrement" => inc.insert(column_name, teon_value_to_bson(&val.neg().unwrap())),
                        "multiply" => mul.insert(column_name, teon_value_to_bson(val)),
                        "divide" => mul.insert(column_name, Bson::Double(val.recip())),
                        "push" => push.insert(column_name, teon_value_to_bson(val)),
                        _ => panic!("Unhandled key."),
                    };
                } else {
                    let bson_val: Bson = BsonCoder::encode(field.field_type(), object.get_value(&key).unwrap())?;
                    if bson_val == Bson::Null {
                        unset.insert(key, bson_val);
                    } else {
                        set.insert(key, bson_val);
                    }
                }
            } else if let Some(property) = model.property(key) {
                let bson_val: Bson = BsonCoder::encode(property.field_type(), object.get_property(&key).await.unwrap())?;
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
            let result = col.update_one(identifier.clone(), update_doc, None).await;
            return match result {
                Ok(_) => Ok(()),
                Err(error) => {
                    Err(self._handle_write_error(&error.kind, object))
                }
            }
        } else {
            let options = FindOneAndUpdateOptions::builder().return_document(ReturnDocument::After).build();
            let result = col.find_one_and_update(identifier.clone(), update_doc, options).await;
            match result {
                Ok(updated_document) => {
                    for key in object.inner.atomic_updater_map.lock().unwrap().keys() {
                        let bson_new_val = updated_document.as_ref().unwrap().get(key).unwrap();
                        let field = object.model().field(key).unwrap();
                        let field_value = BsonCoder::decode(model, field.field_type(), field.is_optional(), bson_new_val, path![])?;
                        object.inner.value_map.lock().unwrap().insert(key.to_string(), field_value);
                    }
                }
                Err(error) => {
                    return Err(self._handle_write_error(&error.kind, object));
                }
            }
        }
        Ok(())
    }

}

#[async_trait]
impl Transaction for MongoDBTransaction {
    async fn migrate(&self, models: Vec<&Model>, reset_database: bool) -> Result<()> {
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
                    let result = model.indices().iter().find(|i| &i.mongodb_name() == name);
                    if result.is_none() {
                        // not in our model definition, but in the database
                        // drop this index
                        let _ = collection.drop_index(name, None).await.unwrap();
                    } else {
                        let result = result.unwrap().as_ref();
                        let our_format_index: Index = (&index).into();
                        if result != &our_format_index {
                            // alter this index
                            // drop first
                            let _ = collection.drop_index(name, None).await.unwrap();
                            // create index
                            let index_options = IndexOptions::builder()
                                .name(result.mongodb_name())
                                .unique(result.r#type() == Type::Unique || result.r#type() == Type::Primary)
                                .sparse(true)
                                .build();
                            let mut keys = doc!{};
                            for item in result.items() {
                                let field = model.field(item.field_name()).unwrap();
                                let column_name = field.column_name();
                                keys.insert(column_name, if item.sort() == Sort::Asc { 1 } else { -1 });
                            }
                            let index_model = IndexModel::builder().keys(keys).options(index_options).build();
                            let _result = collection.create_index(index_model, None).await;
                        }
                    }
                    reviewed_names.push(name.clone());
                }
            }
            for index in model.indices() {
                if !reviewed_names.contains(&index.mongodb_name()) {
                    // ignore primary
                    if index.keys().len() == 1 {
                        let field = model.field(index.keys().get(0).unwrap()).unwrap();
                        if field.column_name() == "_id" {
                            continue
                        }
                    }
                    // create this index
                    let index_options = IndexOptions::builder()
                        .name(index.mongodb_name())
                        .unique(index.r#type() == Type::Unique || index.r#type() == Type::Primary)
                        .sparse(true)
                        .build();
                    let mut keys = doc!{};
                    for item in index.items() {
                        let field = model.field(item.field_name()).unwrap();
                        let column_name = field.column_name();
                        keys.insert(column_name, if item.sort() == Sort::Asc { 1 } else { -1 });
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

    async fn save_object(&self, object: &Object, path: KeyPath) -> teo_runtime::path::Result<()> {
        if object.is_new() {
            self.create_object(object).await
        } else {
            self.update_object(object).await
        }
    }

    async fn delete_object(&self, object: &Object, path: KeyPath) -> teo_runtime::path::Result<()> {
        if object.is_new() {
            return Err(error_ext::object_is_not_saved_thus_cant_be_deleted(path));
        }
        let model = object.model();
        let col = self.get_collection(model);
        let bson_identifier: Bson = teon_value_to_bson(&object.db_identifier());
        let document_identifier = bson_identifier.as_document().unwrap();
        let result = col.delete_one(document_identifier.clone(), None).await;
        return match result {
            Ok(_result) => Ok(()),
            Err(err) => {
                Err(error_ext::unknown_database_delete_error(path, format!("{}", err))),
            }
        }
    }

    async fn find_unique(&self, model: &'static Model, finder: &Value, ignore_select_and_include: bool, action: Action, transaction_ctx: Ctx, req_ctx: Option<teo_runtime::request::Ctx>, path: KeyPath) -> teo_runtime::path::Result<Option<Object>> {
        let select = finder.get("select");
        let include = finder.get("include");
        let aggregate_input = Aggregation::build(transaction_ctx.namespace(), model, finder)?;
        let col = self.get_collection(model);
        let cur = col.aggregate(aggregate_input, None).await;
        if cur.is_err() {
            return Err(Error::unknown_database_find_unique_error());
        }
        let cur = cur.unwrap();
        let results: Vec<std::result::Result<Document, MongoDBError>> = cur.collect().await;
        if results.is_empty() {
            Ok(None)
        } else {
            for doc in results {
                let obj = transaction_ctx.new_object(model, action, req_ctx)?;
                self.clone().document_to_object(&doc.unwrap(), &obj, select, include)?;
                return Ok(Some(obj));
            }
            Ok(None)
        }
    }

    async fn find_many(&self, model: &'static Model, finder: &Value, ignore_select_and_include: bool, action: Action, transaction_ctx: Ctx, req_ctx: Option<teo_runtime::request::Ctx>, path: KeyPath) -> teo_runtime::path::Result<Vec<Object>> {
        let select = finder.get("select");
        let include = finder.get("include");
        let aggregate_input = Aggregation::build(transaction_ctx.namespace(), model, finder)?;
        let reverse = Input::has_negative_take(finder);
        let col = self.get_collection(model);
        // println!("see aggregate input: {:?}", aggregate_input);
        let cur = col.aggregate(aggregate_input, None).await;
        if cur.is_err() {
            println!("{:?}", cur);
            return Err(Error::unknown_database_find_error());
        }
        let cur = cur.unwrap();
        let mut result: Vec<Object> = vec![];
        let results: Vec<std::result::Result<Document, MongoDBError>> = cur.collect().await;
        for doc in results {
            let obj = transaction_ctx.new_object(model, action, req_ctx.clone())?;
            match self.clone().document_to_object(&doc.unwrap(), &obj, select, include) {
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

    async fn count(&self, model: &'static Model, finder: &Value, transaction_ctx: Ctx, path: KeyPath) -> teo_runtime::path::Result<usize> {
        let input = Aggregation::build_for_count(transaction_ctx.namespace(), model, finder)?;
        let col = self.get_collection(model);
        let cur = col.aggregate(input, None).await;
        if cur.is_err() {
            println!("{:?}", cur);
            return Err(Error::unknown_database_find_error());
        }
        let cur = cur.unwrap();
        let results: Vec<std::result::Result<Document, MongoDBError>> = cur.collect().await;
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

    async fn aggregate(&self, model: &'static Model, finder: &Value, transaction_ctx: Ctx, path: KeyPath) -> teo_runtime::path::Result<Value> {
        let results = self.aggregate_or_group_by(transaction_ctx.namespace(), model, finder).await?;
        if results.is_empty() {
            // there is no record
            let mut retval = teon!({});
            for (g, o) in finder.as_hashmap().unwrap() {
                retval.as_hashmap_mut().unwrap().insert(g.clone(), teon!({}));
                for (k, _v) in o.as_hashmap().unwrap() {
                    let value = if g == "_count" { teon!(0) } else { teon!(null) };
                    retval.as_hashmap_mut().unwrap().get_mut(g.as_str()).unwrap().as_hashmap_mut().unwrap().insert(k.to_string(), value);
                }
            }
            Ok(retval)
        } else {
            Ok(results.get(0).unwrap().clone())
        }
    }

    async fn group_by(&self, model: &'static Model, finder: &Value, transaction_ctx: Ctx, path: KeyPath) -> teo_runtime::path::Result<Value> {
        Ok(Value::Array(self.aggregate_or_group_by(transaction_ctx.namespace(), model, finder).await?))
    }

    async fn is_committed(&self) -> bool {
        false
    }

    async fn commit(&self) -> Result<()> {
        Ok(())
    }

    async fn spawn(&self) -> Result<Arc<dyn Transaction>> {
        Ok(Arc::new(self.clone()))
    }
}

unsafe impl Sync for MongoDBTransaction {}
unsafe impl Send for MongoDBTransaction {}
