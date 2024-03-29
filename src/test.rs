// use polars::prelude::*;
// use std::fs::File;
// use std::io::*;

// pub mod helper;

// use rsi::RelativeStrengthIndex;


// fn main() {
//     println!("Hello, world!");

//     // open csv file and print the dataframe
//     let csv_path: &str = "eurusd.csv";

//     let df: DataFrame = csv_to_dataframe(
//         csv_path
//     );

//     // use the rsi function
//     let rsi: RelativeStrengthIndex = RelativeStrengthIndex::new(
//         df,
//         14
//     );

//     // turn the rsi lazyframe into a dataframe and print

//     let rsi_df: DataFrame = rsi.lazyframe.collect().unwrap();
//     println!(
//         "RSI DataFrame:\n{:?}",
//         rsi_df
//     );

//     // write the rsi dataframe to a csv file
//     write_dataframe_to_csv_polars(
//         &mut rsi_df.clone()
//     );



// }


// pub fn csv_to_dataframe(
//     csv_path: &str
// ) -> DataFrame {
//     // Read a CSV file into a DataFrame

//     let file: File = File::open(
//         csv_path
//     ).expect(
//         "Could not open file"
//     );

//     let reader: BufReader<File> = BufReader::new(file);

//     CsvReader::new(reader)
//         .infer_schema(None)
//         .has_header(true)
//         .finish()
//         .unwrap()
// }


// pub fn write_dataframe_to_csv_polars(
//     dataframe: &mut DataFrame
// ) {
//     // Write a DataFrame to a CSV file

//     let file: File = File::create(
//         "output.csv"
//     ).expect(
//         "Could not create file"
//     );

//     let writer: BufWriter<File>  = BufWriter::new(file);

//     CsvWriter::new(writer)
//         .finish(dataframe)
//         .unwrap();
// }
