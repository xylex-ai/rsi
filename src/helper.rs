use std::env;
use polars::prelude::{
    DataFrame,
    LazyFrame,
    PolarsError,
    IntoLazy
};


#[allow(dead_code)]
pub struct Helpers {
    dataframe: DataFrame
}


pub enum ColumnNames {
    Time,
    Close
}

/// We assume that the `time` & `close` columns are present and are named as such, if you want to change the column names,
/// you can do so by setting the environment variables `TIME_COLUMN_NAME` & `CLOSE_COLUMN_NAME` to the desired column names.
impl ColumnNames {
    // Convert enum variants to their string representation
    fn as_str(&self) -> String {
        match self {
            ColumnNames::Time => env::var("TIME_COLUMN_NAME").unwrap_or_else(|_| "time".to_string()),
            ColumnNames::Close => env::var("CLOSE_COLUMN_NAME").unwrap_or_else(|_| "close".to_string()),
        }
    }
}


impl Helpers {
    // Constructor for the dataframe normalizing helper
    pub fn new(
        dataframe: DataFrame
    ) -> Self {
        Self {
            dataframe
        }
    }

    pub fn normalize_dataframe(
        dataframe: DataFrame
    ) -> DataFrame {
        // Normalize a dataframe to where only time and close are left

        let time_column_name: String = ColumnNames::Time.as_str();
        let close_column_name: String = ColumnNames::Close.as_str();

        let dataframe_normalized: DataFrame = dataframe
            .select(vec![
                time_column_name,
                close_column_name
                ])
            .unwrap();

        dataframe_normalized
    }

    pub fn convert_dataframe_to_lazyframe(
        dataframe: DataFrame
    ) -> Result<LazyFrame, PolarsError> {
        // Convert a dataframe to a lazyframe

        Ok(dataframe.lazy())
    }
}