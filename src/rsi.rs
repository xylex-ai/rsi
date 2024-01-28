use std::env;
use polars::prelude::*;

use crate::helper::Helpers;


#[derive(Clone)]
pub struct PriceChange {
    lazyframe: LazyFrame
}


/// Calculate the price change between two rows of the dataframe
impl PriceChange {
    pub fn new(
        lazyframe: LazyFrame
    ) -> Self {
        Self {
            lazyframe
        }
    }

    pub fn calculate_price_change(
        lazyframe: LazyFrame
    ) -> Result<LazyFrame, PolarsError>{

        let lazyframe_price_change: LazyFrame = lazyframe.clone()
            .with_column(
                when(
                    col("close").neq(lit(0.0))
                ).then(
                    col("close") - col("close").shift(lit(1))
                ).otherwise(
                    lit(0.0)
                )
                .alias("price_change")
            ).fill_null(
                lit(0.0)
            );

        Ok(lazyframe_price_change)
    }
}

// use the with column function to calculate the price change between two rows
