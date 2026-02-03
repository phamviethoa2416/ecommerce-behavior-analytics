package com.example.main.batch

import com.example.config.AppConfig
import com.example.handler.RetryHandler
import com.example.util.{GcsUtils, SparkUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.time.LocalDate
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object GOLD_ZONE {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("Gold Zone")

    val gcsSettings = AppConfig.applicationConfig.gcs.getOrElse(
      throw new IllegalStateException("GCS configuration is required.")
    )
    val gcsBucketName = gcsSettings.bucketName
    val workingPath = s"${gcsSettings.basePath}/working_zone"
    val goldBasePath = s"${gcsSettings.basePath}/gold_zone"

    val processingDate = if (args.nonEmpty) args(0) else LocalDate.now().toString

    try {
      // Configure GCS connection
      RetryHandler.withRetry(
        GcsUtils.configureGcs(
          spark,
          gcsSettings.projectId,
          gcsSettings.credentialsPath),
        name = "GCS Configuration"
      ) match {
        case Success(_) => logger.info("GCS configured successfully")
        case Failure(exception) =>
          logger.error("Failed to configure GCS", exception)
          sys.exit(1)
      }

      RetryHandler.withRetry(
        GcsUtils.checkBucketExists(
          gcsSettings.projectId,
          gcsBucketName,
          gcsSettings.credentialsPath),
        name = "GCS Bucket Check/Create"
      ) match {
        case Success(_) => logger.info(s"GCS bucket '$gcsBucketName' is ready")
        case Failure(exception) =>
          logger.error(s"Error while checking/creating GCS bucket '$gcsBucketName'", exception)
          sys.exit(1)
      }

      logger.info(s"Reading Working Zone for ingestion_date = $processingDate")
      val workingDf = Try(
        spark.read.format("delta")
          .load(s"gs://$gcsBucketName/$workingPath")
          .filter(col("ingestion_date") === lit(processingDate))
      ) match {
        case Success(df) =>
          logger.info("Successfully read Working Zone data")
          df
        case Failure(ex) =>
          logger.error("Failed to read Working Zone data", ex)
          throw ex
      }

      val cachedWorkingDf = workingDf.cache()
      val recordCount = cachedWorkingDf.count()

      if (recordCount == 0) {
        logger.warn(s"No records found in Working Zone for ingestion_date=$processingDate. Exiting.")
        cachedWorkingDf.unpersist()
        spark.stop()
        return
      }

      logger.info(s"Read $recordCount records from Working Zone")

      processFactEvents(spark, cachedWorkingDf, gcsBucketName, goldBasePath, processingDate)
      computeDailyEventsAgg(spark, cachedWorkingDf, gcsBucketName, goldBasePath, processingDate)
      computeProductDailyAgg(spark, cachedWorkingDf, gcsBucketName, goldBasePath, processingDate)

      cachedWorkingDf.unpersist()
      logger.info("Gold Zone batch job completed successfully")

    } catch {
      case NonFatal(ex) =>
        logger.error("Gold Zone batch job failed", ex)
        throw ex
    } finally {
      spark.stop()
    }
  }

  /**
   * Processes fact_events table - central fact table with event-level detail.
   * Write mode: APPEND (adds new events to existing table)
   */
  private def processFactEvents(
                                  spark: SparkSession,
                                  workingDf: DataFrame,
                                  bucket: String,
                                  goldBasePath: String,
                                  processingDate: String
                                ): Unit = {
    logger.info(s"Processing fact_events for date=$processingDate")

    val factDf = workingDf.select(
      col("event_date"),
      col("event_time"),
      col("event_type"),
      col("product_id"),
      col("category_id"),
      col("category_code"),
      col("brand"),
      col("price"),
      col("user_id"),
      col("user_session")
    )

    val factPath = s"$goldBasePath/fact_events"

    RetryHandler.withRetry(
      GcsUtils.writeDeltaTable(
        df = factDf,
        bucketName = bucket,
        path = factPath,
        saveMode = SaveMode.Append,
        partitionColumns = Some(Seq("event_date"))
      ),
      name = "Gold Zone fact_events Write"
    ) match {
      case Success(_) =>
        logger.info(s"fact_events: Successfully appended ${factDf.count()} records")
      case Failure(ex) =>
        logger.error("Failed to write fact_events", ex)
        throw ex
    }
  }

  /**
   * Computes agg_daily_events - daily platform-level aggregations.
   * Write mode: OVERWRITE (replaces partition for idempotency)
   */
  private def computeDailyEventsAgg(
                                     spark: SparkSession,
                                     workingDf: DataFrame,
                                     bucket: String,
                                     goldBasePath: String,
                                     processingDate: String
                                   ): Unit = {
    logger.info(s"Computing agg_daily_events for date=$processingDate")

    val dailyAgg = workingDf.groupBy(col("event_date"))
      .agg(
        count("*").alias("total_events"),
        countDistinct("user_session").alias("total_sessions"),
        countDistinct("user_id").alias("total_users"),
        sum(when(col("event_type") === "purchase", col("price")).otherwise(0))
          .cast("decimal(20,2)").alias("total_revenue"),
        sum(when(col("event_type") === "view", 1).otherwise(0)).alias("view_count"),
        sum(when(col("event_type") === "cart", 1).otherwise(0)).alias("cart_count"),
        sum(when(col("event_type") === "purchase", 1).otherwise(0)).alias("purchase_count")
      )

    val aggPath = s"$goldBasePath/agg_daily_events"

    RetryHandler.withRetry(
      GcsUtils.writeDeltaTable(
        df = dailyAgg,
        bucketName = bucket,
        path = aggPath,
        saveMode = SaveMode.Overwrite,
        partitionColumns = Some(Seq("event_date"))
      ),
      name = "Gold Zone agg_daily_events Write"
    ) match {
      case Success(_) =>
        logger.info(s"agg_daily_events: Successfully wrote aggregation")
      case Failure(ex) =>
        logger.error("Failed to write agg_daily_events", ex)
        throw ex
    }
  }

  /**
   * Computes agg_product_daily - product performance by day.
   * Write mode: OVERWRITE (replaces partition for idempotency)
   */
  private def computeProductDailyAgg(
                                      spark: SparkSession,
                                      workingDf: DataFrame,
                                      bucket: String,
                                      goldBasePath: String,
                                      processingDate: String
                                    ): Unit = {
    logger.info(s"Computing agg_product_daily for date=$processingDate")

    // Get the last non-null category_code and brand for each product using window function
    import org.apache.spark.sql.expressions.Window
    val productWindow = Window.partitionBy("product_id", "event_date")
      .orderBy(col("event_time").desc)

    val enrichedDf = workingDf
      .withColumn("row_num", row_number().over(productWindow))
      .filter(col("row_num") === 1)
      .select("product_id", "event_date", "category_code", "brand")

    val productAgg = workingDf.groupBy(
      col("event_date"),
      col("product_id")
    ).agg(
      sum(when(col("event_type") === "view", 1).otherwise(0)).alias("views"),
      sum(when(col("event_type") === "cart", 1).otherwise(0)).alias("cart_adds"),
      sum(when(col("event_type") === "purchase", 1).otherwise(0)).alias("purchases"),
      sum(when(col("event_type") === "purchase", col("price")).otherwise(0))
        .cast("decimal(20,2)").alias("revenue")
    )

    // Join with enriched data to get category_code and brand
    val productDailyAgg = productAgg.join(
      enrichedDf,
      Seq("event_date", "product_id"),
      "left"
    ).select(
      col("event_date"),
      col("product_id"),
      col("category_code"),
      col("brand"),
      col("views"),
      col("cart_adds"),
      col("purchases"),
      col("revenue")
    )

    val aggPath = s"$goldBasePath/agg_product_daily"

    RetryHandler.withRetry(
      GcsUtils.writeDeltaTable(
        df = productDailyAgg,
        bucketName = bucket,
        path = aggPath,
        saveMode = SaveMode.Overwrite,
        partitionColumns = Some(Seq("event_date"))
      ),
      name = "Gold Zone agg_product_daily Write"
    ) match {
      case Success(_) =>
        logger.info(s"agg_product_daily: Successfully wrote aggregation")
      case Failure(ex) =>
        logger.error("Failed to write agg_product_daily", ex)
        throw ex
    }
  }
}
