import findspark
import sys
import constants
import classes
findspark.init()

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from typing import Iterable
from pyspark.sql.types import *
import pyspark.sql.functions as func
from pyspark.sql.functions import *


ERRONEOUS_DIR = "erroneous"
AGGREGATED_DIR = "aggregated"


def get_raw_bids(session, bids_path):
    schema = StructType([StructField(constants.BIDS_HEADER[0], IntegerType()),
                         StructField(constants.BIDS_HEADER[1], TimestampType()),
                         StructField(constants.BIDS_HEADER[2], StringType()),
                         StructField(constants.BIDS_HEADER[3], DoubleType()),
                         StructField(constants.BIDS_HEADER[4], DoubleType()),
                         StructField(constants.BIDS_HEADER[5], DoubleType()),
                         StructField(constants.BIDS_HEADER[6], DoubleType()),
                         StructField(constants.BIDS_HEADER[7], DoubleType()),
                         StructField(constants.BIDS_HEADER[8], DoubleType()),
                         StructField(constants.BIDS_HEADER[9], DoubleType()),
                         StructField(constants.BIDS_HEADER[10], DoubleType()),
                         StructField(constants.BIDS_HEADER[11], DoubleType()),
                         StructField(constants.BIDS_HEADER[12], DoubleType()),
                         StructField(constants.BIDS_HEADER[13], DoubleType()),
                         StructField(constants.BIDS_HEADER[14], DoubleType()),
                         StructField(constants.BIDS_HEADER[15], DoubleType()),
                         StructField(constants.BIDS_HEADER[16], DoubleType()),
                         StructField(constants.BIDS_HEADER[17], DoubleType())])

    bids_df = session.read \
        .option("delimiter", constants.DELIMITER) \
        .option("timestampFormat", "HH-dd-MM-yyyy") \
        .option("header", "false") \
        .schema(schema) \
        .csv(bids_path)
    return bids_df
    pass


def get_erroneous_records(bids):
    erroneous_records_df = bids.filter(bids.HU.contains('ERROR_'))

    erroneous_records_df = erroneous_records_df.select(
        constants.BIDS_HEADER[1], constants.BIDS_HEADER[2])

    erroneous_records_df = erroneous_records_df.groupBy(erroneous_records_df.columns).count()

    return erroneous_records_df
    pass


def get_exchange_rates(session, path):
    schema = StructType([StructField(constants.EXCHANGE_RATES_HEADER[0], TimestampType()),
                         StructField(constants.EXCHANGE_RATES_HEADER[1], StringType()),
                         StructField(constants.EXCHANGE_RATES_HEADER[2], StringType()),
                         StructField(constants.EXCHANGE_RATES_HEADER[3], DoubleType())])

    exchange_rates_df = session.read \
        .option("delimiter", constants.DELIMITER) \
        .option("timestampFormat", "HH-dd-MM-yyyy") \
        .option("header", "false")\
        .schema(schema) \
        .csv(path)

    exchange_rates_dict = exchange_rates_df.select("ValidFrom", "ExchangeRate").rdd.collectAsMap()

    #exchange_rates_dict = exchange_rates.rdd.map(lambda x: {x.ValidFrom, x.ExchangeRate})
    #print(exchange_rates_dict.take(10))

    return exchange_rates_dict
    pass


def get_bids(bids, rates):
    bids_df = get_bids_without_error(bids)
    bids_df = get_bids_select_countries(bids_df)
    bids_df = get_bids_rows(bids_df)

    #convert_usd_to_eur(bids_df, rates)

    return bids_df
    pass


def get_bids_without_error(bids):
    bids_without_error_df = bids.filter(~ bids.HU.contains('ERROR_'))
    return bids_without_error_df
    pass


def get_bids_select_countries(bids):
    bids_select_countries_df = bids.select(
        constants.BIDS_HEADER[0], constants.BIDS_HEADER[1], constants.TARGET_LOSAS[0],
        constants.TARGET_LOSAS[1], constants.TARGET_LOSAS[2])

    return bids_select_countries_df
    pass


def get_bids_rows(bids):
    bids_rows_df = bids.selectExpr("MotelID", "BidDate", "stack(5, 'US', US, 'CA', CA, 'MX', MX)")\
        .withColumnRenamed("col0", "Losa")\
        .withColumnRenamed("col1", "Price")\
        .where("Price is not null")

    return bids_rows_df
    pass


def convert_usd_to_eur(bids, rates):
    bids.show()

    #bids.foreach(lambda row: get_bid_item(row))

    #print(bids_items_list.__str__())

    #bids.foreach(lambda x: row_update1(x, rates))

    #bids.

    bids_udf = udf(get_update_bids, DoubleType())


    bids2 = bids.withColumn('Price', lit(bids_udf(bids.Price, rates.get(bids['BidDate']))))

    #bids2 = bids.withColumn('Value', row_update(col('BidDate'), col('Value'), rates))

    #bids.foreach(lambda x: (row_update(x.BidDate, x.Value, rates)))

    print("________________________")
    bids2.show()

    #update_bids = bids.withColumn("Value", when(not rates.get(col("BidDate"))  == none, col("Value") * rates.get(col("BidDate"))).otherwise(col("Value")))

    #update_bids.show()

    return bids
    pass


def get_update_bids(original_value, exchange_rate):
    return original_value * exchange_rate
    pass


def get_bid_item(row):
    bid_item = classes.BidItem(row.MotelID, row.BidDate, row.Losas, row.Value)
    bids_items_list.extend(bid_item)
    pass


def row_update1(row, rates):
    value = rates.get(row.BidDate)
    if value is not None:
        row.Value *= value

    pass


def row_update(date, original_value, rates):
    value = rates.get(date)
    if value is not None:
        original_value *= value
        return value
    else:
        return original_value

    pass


def get_motels(session, path):
    schema = StructType([StructField(constants.MOTELS_HEADER[0], IntegerType()),
                         StructField(constants.MOTELS_HEADER[1], StringType()),
                         StructField(constants.MOTELS_HEADER[2], StringType()),
                         StructField(constants.MOTELS_HEADER[3], StringType()),
                         StructField(constants.MOTELS_HEADER[4], StringType())])

    motels_df = session.read \
        .option("delimiter", constants.DELIMITER) \
        .option("header", "false") \
        .schema(schema) \
        .csv(path)

    return motels_df
    pass


def get_enriched(bids, motel):
    bids_motels_df = bids.join(motel, bids['MotelID'] == motel['MotelID'], 'inner')\
        .select(motel['MotelID'], motel['MotelName'], bids['BidDate'], bids['Losa'], bids['Price'])

    bids_maximum_df = bids_motels_df.groupBy("MotelID", "BidDate")\
        .agg(max("Price").alias('Price_copy'))\
        .withColumnRenamed("MotelID", 'MotelID_copy')\
        .withColumnRenamed("BidDate", 'BidDate_copy')

    enriched_df = bids_motels_df.join(bids_maximum_df, (bids_maximum_df['MotelID_copy'] == bids_motels_df['MotelID'])
                               & (bids_maximum_df['BidDate_copy'] == bids_motels_df['BidDate'])
                               & (bids_maximum_df['Price_copy'] == bids_motels_df['Price']), 'inner')\
        .select(bids_motels_df['MotelID'], bids_motels_df['MotelName'], bids_motels_df['BidDate'],
                bids_motels_df['Losa'], bids_motels_df['Price'])\
        .sort(desc('Price'))
    return enriched_df
    pass


def process_data(session, bids_path, motels_path, exchange_rates_path,
                 output_base_path):
    """
    Task 1:
        * Read the bid data from the provided file
      """
    raw_bids = get_raw_bids(session, bids_path)
    raw_bids.show(10)

    """
    * Task 2:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
    """
    erroneous_records = get_erroneous_records(raw_bids)
    erroneous_records.show(10)

    erroneous_records \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .csv(output_base_path + "/" + ERRONEOUS_DIR)

    """
    * Task 3:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
    """
    exchange_rates = get_exchange_rates(session, exchange_rates_path)

    """
    * Task 4:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
    """

    bids = get_bids(raw_bids, exchange_rates)
    bids.show(10)

    """
    * Task 5:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
    """
    motels = get_motels(spark_session, motels_path)
    motels.show(10)

    """
    * Task6:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price
    """

    enriched = get_enriched(bids, motels)
    enriched.show(10)
    enriched.coalesce(1) \
        .write\
        .mode("overwrite")\
        .csv(output_base_path + "/" + AGGREGATED_DIR)


if __name__ == '__main__':
    spark_session = SparkSession \
        .builder\
        .appName("motels-homework") \
        .master("local[*]") \
        .getOrCreate()

    if len(sys.argv) == 5:
        bidsPath = sys.argv[1]
        motelsPath = sys.argv[2]
        exchangeRatesPath = sys.argv[3]
        outputBasePath = sys.argv[4]
    else:
        raise ValueError("Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    # process_data(spark_session, "../resources/small_dataset/bids.txt", "../resources/small_dataset/motels.txt",
    #             "../resources/small_dataset/exchange_rate.txt", "../resources/output")

    process_data(spark_session, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    spark_session.stop()

#"../resources/small_dataset/bids.txt" "../resources/small_dataset/motels.txt" "../resources/small_dataset/exchange_rate.txt" "../resources/output"