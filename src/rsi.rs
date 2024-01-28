use std::env;
use polars::prelude::*;


#[derive(Clone)]
#[allow(dead_code)]
pub struct PriceChange {
    lazyframe: LazyFrame
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct RollingMean {
    lazyframe: LazyFrame,
    period: i64
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct FinalRS {
    lazyframe: LazyFrame
}


#[derive(Clone)]
#[allow(dead_code)]
pub struct FinalRSI {
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
        let column_close_name: &str  = &env::var("CLOSE_COLUMN_NAME").unwrap_or_else(|_| "close".to_string());

        let lazyframe_price_change: LazyFrame = lazyframe.clone()
            .with_column(
                when(
                    col(column_close_name).neq(lit(0.0))
                ).then(
                    col(column_close_name) - col(column_close_name).shift(lit(1))
                ).otherwise(
                    lit(0.0)
                )
                .alias("price_change")
            ).fill_null(
                lit(0.0)
            )

            // move all negative values to price_change_negative, and all positive values to price_change_positive
            .with_columns(
                vec![
                    when(
                        col("price_change").lt(lit(0.0))
                    ).then(
                        col("price_change")
                    ).otherwise(
                        lit(0.0)
                    )
                    .alias("price_change_negative"),

                    when(
                        col("price_change").gt(lit(0.0))
                    ).then(
                        col("price_change")
                    ).otherwise(
                        lit(0.0)
                    )
                    .alias("price_change_positive")
                ]
                );

        Ok(lazyframe_price_change)
    }
}


// use the with column function to calculate the price change between two rows

impl RollingMean {
    pub fn new(
        lazyframe: LazyFrame,
        period: i64
    ) -> Self {
        Self {
            lazyframe,
            period
        }
    }

    // calculate the rolling mean of the price change to construct RS
    pub fn period_rolling_mean(
        lazyframe: LazyFrame,
        period: i64
    ) -> Result<LazyFrame, PolarsError> {
        let dataframe: DataFrame = lazyframe.collect()?;

        let series_change_negative: Series = dataframe.column("price_change_negative")?.clone();
        let series_change_positive: Series = dataframe.column("price_change_positive")?.clone();

        let series_change_negative: Series = series_change_negative.rolling_mean(
            RollingOptions::into(
                RollingOptions {
                    window_size: polars::prelude::Duration::new(period),
                    min_periods: 1,
                    center: false,
                    weights: None,
                    by: None,
                    closed_window: None,
                    fn_params: None,
                    warn_if_unsorted: false,
            }))?;

        let series_change_positive: Series = series_change_positive.rolling_mean(
            RollingOptions::into(
                RollingOptions {
                    window_size: polars::prelude::Duration::new(period),
                    min_periods: 1,
                    center: false,
                    weights: None,
                    by: None,
                    closed_window: None,
                    fn_params: None,
                    warn_if_unsorted: false,
            }))?;


        let dataframe: DataFrame = dataframe.clone();

        // rename series
        let mut series_change_negative: Series = series_change_negative;
        let mut series_change_positive: Series = series_change_positive;

        series_change_negative.rename("rolling_mean_negative");
        series_change_positive.rename("rolling_mean_positive");

        // hstack
        let dataframe_rolling: DataFrame = dataframe.hstack(
            &[
                series_change_negative,
                series_change_positive
            ])?;

        let lazyframe_rolling: LazyFrame = dataframe_rolling.lazy();

        Ok(lazyframe_rolling)
    }

}


impl FinalRS {
    pub fn new(
        lazyframe: LazyFrame,
    ) -> Self {
        Self {
            lazyframe
        }
    }

    pub fn calculate_final_rs(
        lazyframe: LazyFrame,
    ) -> Result<LazyFrame, PolarsError> {
        let mut dataframe: DataFrame = lazyframe.clone().collect()?;

        fn negative_to_absolute(str_val: &Series) -> Series {
            str_val.f64()
                .unwrap()
                .into_iter()
                .map(|opt_val| {
                    opt_val.map(|val| {
                        let val: f64 = 100000.0 - (val + 100000.0);
                        val
                    })
                })
                .collect::<Float64Chunked>()
                .into_series()
        }

        let absolute_negative: &mut DataFrame = dataframe.apply(
            "rolling_mean_negative",
            negative_to_absolute
        )?;



        // divide the average gain (rolling_mean_positive) by the average loss (rolling_mean_negative) to get RS
        let series_rolling_mean_positive: Series = absolute_negative.column("rolling_mean_positive")?.clone();
        let series_rolling_mean_negative: Series = absolute_negative.column("rolling_mean_negative")?.clone();

        // calculate RS by dividing the average gain (rolling_mean_positive) by the average loss (rolling_mean_negative)
        let mut series_rs: Series = series_rolling_mean_positive.f64()
            .unwrap()
            .into_iter()
            .map(|opt_val| {
                opt_val.map(|val| {
                    let val: f64 = val / series_rolling_mean_negative.f64().unwrap().get(0).unwrap();
                    val
                })
            })
            .collect::<Float64Chunked>()
            .into_series();

        // rename the series to rs
        let series_rs_renamed: &mut Series = series_rs.rename("rs");
        // unmut the series
        let series_rs_renamed: Series = series_rs_renamed.clone();

        // add RS to dataframe
        let absolute_negative: &mut DataFrame = absolute_negative.with_column(
            series_rs_renamed
        )?;


        // conver to lazyframe
        let lazyframe: LazyFrame = absolute_negative.clone().lazy();

        Ok(lazyframe.clone())
    }
}


// calculate the final RS
// RSI = 100 - (100 / (1 + RS))
impl FinalRSI {
    pub fn new(
        lazyframe: LazyFrame,
    ) -> Self {
        Self {
            lazyframe
        }
    }

    pub fn calculate_final_rsi(
        lazyframe: LazyFrame,
    ) -> Result<LazyFrame, PolarsError> {
        let dataframe: DataFrame = lazyframe.collect()?;

        let series_rolling_mean_positive: Series = dataframe.column("rolling_mean_positive")?.clone();
        let series_rolling_mean_negative: Series = dataframe.column("rolling_mean_negative")?.clone();

        let mut series_rolling_mean_negative: Series = series_rolling_mean_negative.f64()
            .unwrap()
            .into_iter()
            .map(|opt_val| {
                opt_val.map(|val| {
                    let val: f64 = 100.0 - (100.0 / (1.0 + (val / series_rolling_mean_positive.f64().unwrap().get(0).unwrap())));
                    val
                })
            })
            .collect::<Float64Chunked>()
            .into_series();

        // rename the series to rsi
        let series_rolling_mean_negative: &mut Series = series_rolling_mean_negative.rename("rsi");

        // unmutate the series
        let series_rolling_mean_negative_unmut: Series = series_rolling_mean_negative.clone();

        let dataframe: DataFrame = dataframe.hstack(
            &[
                series_rolling_mean_negative_unmut
            ])?;

        let lazyframe: LazyFrame = dataframe.lazy();

        Ok(lazyframe)
    }
}