package com.pg
import com.pg.utils._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

object SourceDataLoading {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Sftp File to Dataframe")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)
    import spark.implicits._

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")
    val srcList = rootConfig.getStringList("source_list").toList
    val redshiftConfig = rootConfig.getConfig("redshift_conf")

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    for (src <- srcList) {
      val srcConfig = rootConfig.getConfig(src)
      // log this into dynamo db <system, run_date, status>, e.g, <SB, 2020-08-27, STARTED>
      src match {
        case "OL" =>

          val olDf = Utility.readSftpData(spark, srcConfig.getConfig("sftp_conf"), srcConfig.getString("file_name"))
            .withColumn("inst_dt", current_date())

          olDf.write
            .partitionBy("inst_dt")
            .mode(SaveMode.Overwrite)
            .parquet(s"s3n://${s3Config.getString("s3_bucket")}/staging_data/$src")


        case "SB" =>
          val sbDf = Utility.readSqlData(spark, srcConfig.getConfig("mysql_conf"), srcConfig.getString("table_name"))
            .withColumn("inst_dt", current_date())
          sbDf.show()

          sbDf.write
            .partitionBy("inst_dt")
            .mode(SaveMode.Overwrite)
            .parquet(s"s3n://${s3Config.getString("s3_bucket")}/staging_data/$src")


        case "1CP" =>

          val s3Df = Utility.reads3Data(spark, srcConfig.getConfig("s3_conf_new"), srcConfig.getString("file_name"))
            .withColumn("inst_dt",current_date())

         s3Df.write
            .partitionBy("inst_dt")
            .mode(SaveMode.Overwrite)
            .parquet(s"s3n://${s3Config.getString("s3_bucket")}/staging_data/$src")


        case "CUST_ADDR" =>
          val newjsonDf = Utility.readjsonData(spark, srcConfig.getConfig("s3_conf_new"), srcConfig.getString("file_name"))
            .withColumn("inst_dt",current_date())

        val newdf=  newjsonDf.select($"consumer_id", $"mobile-no", $"address.city".as("city"),
          $"address.state".as("state"),$"address.street".as("street"),$"inst_dt")


          newdf.write.
            partitionBy("inst_dt")
            .mode(SaveMode.Overwrite)
            .parquet(s"s3n://${s3Config.getString("s3_bucket")}/staging_data/$src")

      }
      // update this into dynamo db <system, run_date, status>, e.g, <SB, 2020-08-27, COMPLETED>


    }

  }
}
