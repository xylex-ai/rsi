//! Relative Strength Index for technical analysis.
//!
//! Written by Xylex AI, 2024
//!
use polars::prelude::*;

pub mod rsi;
pub mod errors;
pub mod helper;

use helper::Helpers;
use rsi::{
    PriceChange,
    RollingMean,
    FinalRS,
    FinalRSI
};


#[derive(Clone)]
pub struct RelativeStrengthIndexLazyFrame {
    pub lazyframe: LazyFrame,
    pub period: usize
}


#[derive(Clone)]
pub struct RelativeStrengthIndexDataFrame {
    pub dataframe: DataFrame,
    pub period: usize
}


impl RelativeStrengthIndexLazyFrame {
    pub fn new(
        lazyframe: LazyFrame,
        period: usize
    ) -> Self {
        Self {
            lazyframe,
            period
        }
    }
}


impl RelativeStrengthIndexDataFrame {
    pub fn new(
        dataframe: DataFrame,
        period: usize
    ) -> Self {
        let lazyframe: LazyFrame = Helpers::convert_dataframe_to_lazyframe(dataframe)
            .expect("Failed to convert DataFrame to LazyFrame");
        Self {
            dataframe: lazyframe.collect().unwrap(),
            period
        }
    }
}


#[derive(Clone)]
pub struct RelativeStrengthIndex {
    pub lazyframe: LazyFrame,
    pub period: usize
}


impl RelativeStrengthIndex {
    pub fn new(
        dataframe: DataFrame,
        period: usize
    ) -> Self {

        let dataframe_normalized: DataFrame = Helpers::normalize_dataframe(dataframe.clone());
        let lazyframe: LazyFrame = Helpers::convert_dataframe_to_lazyframe(dataframe_normalized)
            .expect("Failed to convert DataFrame to LazyFrame");

        let lazyframe: LazyFrame = PriceChange::calculate_price_change(lazyframe.clone())
            .expect("Failed to calculate price change");

        let lazyframe: LazyFrame = RollingMean::period_rolling_mean(lazyframe.clone(), period as i64)
            .expect("Failed to calculate rolling mean");

        let lazyframe: LazyFrame = FinalRS::calculate_final_rs(lazyframe.clone())
            .expect("Failed to calculate final RS");

        // let lazyframe: LazyFrame = FinalRSI::calculate_final_rsi(lazyframe.clone()).expect("Failed to calculate final RSI");

        // let lazyframe: LazyFrame = remerge_dataframe(
        //     dataframe.clone(),
        //     lazyframe.clone().collect().unwrap()
        // ).expect("Failed to remerge dataframe");

        Self {
            lazyframe,
            period
        }
    }
}


fn remerge_dataframe(
    dataframe: DataFrame,
    dataframe_normalized: DataFrame
) -> Result<LazyFrame, PolarsError> {
    // remerge the normalized dataframe with the original dataframe

    // take the rsi column from the normalized dataframe and add it to the original dataframe

    let rsi_column: Series = dataframe_normalized.column("rsi").unwrap().clone();

    // hstack
    let dataframe: DataFrame = dataframe.hstack(
        &[
            rsi_column
        ])?;

    let lazyframe: LazyFrame = dataframe.lazy();

    Ok(lazyframe)
}