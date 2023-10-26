use bson::Bson;
use indexmap::IndexMap;
use key_path::KeyPath;
use teo_result::{Error, Result};
use teo_parser::r#type::Type;
use teo_runtime::model::field::named::Named;
use teo_teon::Value;
use teo_runtime::model::Model;

pub(crate) struct BsonCoder { }

impl BsonCoder {

    pub(crate) fn encode_without_default_type(value: &Value) -> Bson {
        value.clone().into()
    }

    pub(crate) fn encode<'a>(r#type: &Type, value: Value) -> Result<Bson> {
        match r#type {
            Type::Int => if let Some(i) = value.as_i32() {
                Ok(Bson::Int32(i))
            } else {
                Ok(Bson::Null)
            },
            Type::Int64 => if let Some(i) = value.as_i64() {
                Ok(Bson::Int64(i))
            } else {
                Ok(Bson::Null)
            },
            _ => Ok(value.into()),
        }
    }

    pub(crate) fn decode<'a>(model: &Model, r#type: &Type, optional: bool, bson_value: &Bson, path: impl AsRef<KeyPath>) -> Result<Value> {
        if bson_value.as_null().is_some() && optional {
            return Ok(Value::Null);
        }
        let path = path.as_ref();
        match r#type {
            Type::ObjectId => match bson_value.as_object_id() {
                Some(oid) => Ok(Value::ObjectId(oid)),
                None => Err(Error::record_decoding_error(model.name(), path, "object id")),
            }
            Type::Bool => match bson_value.as_bool() {
                Some(b) => Ok(Value::Bool(b)),
                None => Err(Error::record_decoding_error(model.name(), path, "bool")),
            }
            Type::I32 => match bson_value.as_i32() {
                Some(n) => Ok(Value::I32(n)),
                None => Err(Error::record_decoding_error(model.name(), path, "int 32")),
            }
            Type::I64 => match bson_value.as_i64() {
                Some(n) => Ok(Value::I64(n)),
                None => Err(Error::record_decoding_error(model.name(), path, "int 64")),
            }
            Type::F32 => match bson_value.as_f64() {
                Some(n) => Ok(Value::F32(n as f32)),
                None => Err(Error::record_decoding_error(model.name(), path, "double")),
            }
            Type::F64 => match bson_value.as_f64() {
                Some(n) => Ok(Value::F64(n)),
                None => Err(Error::record_decoding_error(model.name(), path, "double")),
            }
            Type::Decimal => panic!("Decimal is not implemented by MongoDB."),
            Type::String => match bson_value.as_str() {
                Some(s) => Ok(Value::String(s.to_owned())),
                None => Err(Error::record_decoding_error(model.name(), path, "string")),
            }
            Type::Date => match bson_value.as_datetime() {
                Some(val) => Ok(Value::Date(val.to_chrono().date_naive())),
                None => Err(Error::record_decoding_error(model.name(), path, "datetime")),
            }
            Type::DateTime => match bson_value.as_datetime() {
                Some(val) => Ok(Value::DateTime(val.to_chrono())),
                None => Err(Error::record_decoding_error(model.name(), path, "datetime")),
            }
            Type::EnumVariant(_, _) => match bson_value.as_str() {
                Some(val) => {
                    let enum_def = r#type.unwrap_enum();
                    let enum_name = enum_def.name();
                    let graph = AppCtx::get().unwrap().graph();
                    if graph.enum_values(enum_name).unwrap().contains(&val.to_string()) {
                        Ok(Value::String(val.to_owned()))
                    } else {
                        Err(Error::record_decoding_error(model.name(), path, format!("string value for enum `{enum_name}'")))
                    }
                },
                None => Err(Error::record_decoding_error(model.name(), path, "string")),
            }
            Type::Array(inner_field) => {
                match bson_value.as_array() {
                    Some(arr) => Ok(Value::Array(arr.iter().enumerate().map(|(i, v)| {
                        let path = path + i;
                        Self::decode(model, inner_field.field_type(), inner_field.is_optional(), v, path)
                    }).collect::<Result<Vec<Value>>>()?)),
                    None => Err(Error::record_decoding_error(model.name(), path, "array")),
                }
            }
            Type::Dictionary(inner_field) => {
                match bson_value.as_document() {
                    Some(doc) => Ok(Value::Dictionary(doc.iter().map(|(k, v)| {
                        let path = path + k;
                        Ok((k.to_owned(), Self::decode(model, inner_field.field_type(), inner_field.is_optional(), v, path)?))
                    }).collect::<Result<IndexMap<String, Value>>>()?)),
                    None => Err(Error::record_decoding_error(model.name(), path, "document")),
                }
            }
        }
    }
}
