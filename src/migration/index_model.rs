use mongodb::IndexModel;
use teo_runtime::sort::Sort;
use teo_runtime::model::{Index, index::Item};
use teo_runtime::model::index::Type;

pub trait FromIndexModel {
    fn from_index_model(index_model: &IndexModel) -> Self;
}

impl FromIndexModel for Index {
    fn from_index_model(index_model: &IndexModel) -> Self {
        let unique_result = index_model.options.as_ref().unwrap().unique;
        let unique = unique_result.unwrap_or(false);
        let mut items: Vec<Item> = Vec::new();
        for (k, v) in &index_model.keys {
            let item = Item::new(k.clone(), if v.as_i32().unwrap() == 1 { Sort::Asc } else { Sort::Desc }, None);
            items.push(item);
        }
        Index::new(if unique { Type::Unique } else { Type::Index }, index_model.options.as_ref().unwrap().name.as_ref().unwrap().to_string(), items)
    }
}
