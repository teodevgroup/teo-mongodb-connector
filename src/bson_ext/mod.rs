use bson::Bson;
use bson::datetime::{DateTime as BsonDateTime};
use chrono::{NaiveDateTime, NaiveTime, TimeZone, Utc};
use teo_teon::Value;

pub(crate) mod coder;

pub(crate) fn teon_value_to_bson(value: &Value) -> Bson {
    match value {
        Value::Null => Bson::Null,
        Value::ObjectId(oid) => Bson::ObjectId(oid.clone()),
        Value::Bool(b) => Bson::Boolean(*b),
        Value::Int(i) => Bson::Int32(*i),
        Value::Int64(i) => Bson::Int64(*i),
        Value::Float32(f) => Bson::Double(*f as f64),
        Value::Float(f) => Bson::Double(*f),
        Value::Decimal(_d) => panic!("Decimal is not implemented by MongoDB."),
        Value::String(s) => Bson::String(s.clone()),
        Value::Date(val) => Bson::DateTime(BsonDateTime::from(Utc.from_utc_datetime(&NaiveDateTime::new(val.clone(), NaiveTime::default())))),
        Value::DateTime(val) => Bson::DateTime(BsonDateTime::from(*val)),
        Value::Array(val) => Bson::Array(val.iter().map(|i| { teon_value_to_bson(i) }).collect()),
        Value::Dictionary(val) => Bson::Document(val.iter().map(|(k, v)| (k.clone(), teon_value_to_bson(v))).collect()),
        Value::EnumVariant(val) => Bson::String(val.value.clone()),
        _ => panic!("Cannot convert to Bson value.")
    }
}
