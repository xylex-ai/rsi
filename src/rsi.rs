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
                    .alias("loss"),

                    when(
                        col("price_change").gt(lit(0.0))
                    ).then(
                        col("price_change")
                    ).otherwise(
                        lit(0.0)
                    )
                    .alias("gain")
                ]
                )
                // add normalized loss column
                .with_column(
                    when(
                        col("loss").lt(lit(0.0))
                    ).then(
                        -col("loss")
                    ).otherwise(
                        lit(0.0)
                    )
                    .alias("loss_normalized")
                );

        // Convert LazyFrame to DataFrame
        let df: DataFrame = lazyframe_price_change.collect()?;

        // Drop the first row
        let new_df = df.slice(1, df.height() - 1);

        // Convert DataFrame back to LazyFrame
        let lazyframe_price_change = new_df.lazy();

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

        let series_loss: Series = dataframe.column("loss_normalized")?.clone();
        let series_gain: Series = dataframe.column("gain")?.clone();

        let loss_average: Series = series_loss.rolling_mean(
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

        let gain_average: Series = series_gain.rolling_mean(
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
        let mut series_change_negative: Series = loss_average;
        let mut series_change_positive: Series = gain_average;

        series_change_negative.rename("loss_average");
        series_change_positive.rename("gain_average");

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

        let lazyframe_rs: LazyFrame = lazyframe.clone()
            .with_column(
                when(
                    col("gain_average").neq(lit(0.0)).and(col("loss_average").neq(lit(0.0)))
                ).then(
                    col("gain_average") / col("loss_average")
                ).otherwise(
                    lit(0.0)
                )
                .alias("rs")
            );


        Ok(lazyframe_rs.clone())
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

        let lazyframe_rsi: LazyFrame = lazyframe.clone()
            .with_column(
                when(
                    col("rs").neq(lit(0.0))
                ).then(
                    lit(100.0) - (lit(100.0) / (lit(1.0) + col("rs")))
                ).otherwise(
                    lit(0.0)
                )
                .alias("rsi")
            );
        Ok(lazyframe_rsi)
    }
}