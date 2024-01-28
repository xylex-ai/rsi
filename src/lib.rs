//! Relative Strength Index for technical analysis.
//!
//! Written by Xylex AI, 2024
//!
use polars::prelude::{
    DataFrame,
    LazyFrame
};

pub mod rsi;
pub mod errors;
pub mod helper;

use helper::Helpers;
use rsi::{
    PriceChange,
    RollingMean,
    FinalRS
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
        let dataframe_normalized: DataFrame = Helpers::normalize_dataframe(dataframe);
        let lazyframe: LazyFrame = Helpers::convert_dataframe_to_lazyframe(dataframe_normalized)
            .expect("Failed to convert DataFrame to LazyFrame");

        let lazyframe: LazyFrame = PriceChange::calculate_price_change(lazyframe.clone())
            .expect("Failed to calculate price change");

        let lazyframe: LazyFrame = RollingMean::period_rolling_mean(lazyframe.clone(), period as i64)
            .expect("Failed to calculate rolling mean");


        let lazyframe: LazyFrame = FinalRS::calculate_final_rs(lazyframe.clone())
            .expect("Failed to calculate final RS");


        Self {
            lazyframe,
            period
        }
    }
}