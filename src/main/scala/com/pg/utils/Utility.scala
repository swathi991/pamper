package com.pg.utils

import com.typesafe.config.Config
import org.apache.spark.sql.functions.current_date
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}



object Utility {

  def readSftpData(spark: SparkSession, sftpConfig: Config, filename: String): DataFrame = {
    spark.read
      .format("com.springml.spark.sftp")
      .option("host", sftpConfig.getString("hostname"))
      .option("port", sftpConfig.getString("port"))
      .option("username", sftpConfig.getString("username"))
      .option("pem", sftpConfig.getString("pem"))
      .option("fileType", "csv")
      .option("delimiter", "|")
      .load(s"${sftpConfig.getString("directory")}/$filename")

  }

  def readSqlData(spark: SparkSession, mysqlConfig: Config, tablename: String): DataFrame = {
    var jdbcParams = Map("url" -> Constants.getMysqlJdbcUrl(mysqlConfig),
      "lowerBound" -> "1",
      "upperBound" -> "100",
      "dbtable" -> s"${mysqlConfig.getString("database")}.$tablename",
      "numPartitions" -> "2",
      "partitionColumn" -> "App_Transaction_Id",
      "user" -> mysqlConfig.getString("username"),
      "password" -> mysqlConfig.getString("password")
    )

    val mydata = spark
      .read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .options(jdbcParams)
      .load()
    mydata
  }

  def reads3Data(spark: SparkSession, s3Config: Config, filename: String): DataFrame = {
    spark.read
      .option("fs.s3n.awsSecretAccessKey", s3Config.getString("access_key"))
      .option("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))
      .option("header", "true")
      .option("delimiter", "|")
      .format("csv")
      .load(s"s3n://${s3Config.getString("s3_bucket")}/$filename")

  }

  def readjsonData(spark: SparkSession, s3Config: Config, filename: String): DataFrame = {

    spark.
      read.
      json(s"s3n://${s3Config.getString("s3_bucket")}/$filename")
  }


  def read1CP(spark: SparkSession, s3config: Config): DataFrame = {

    spark.read.parquet(s"s3n://${s3config.getString("s3_bucket")}/staging_data/1CP")
      .withColumn("inst_dt",current_date())
  }


  def readSBStg(spark: SparkSession, s3config: Config): DataFrame = {

    spark.read.parquet(s"s3n://${s3config.getString("s3_bucket")}/staging_data/SB")
      .withColumn("inst_dt",current_date())
  }

  def readOLStg(spark: SparkSession, s3config: Config): DataFrame = {

    spark.read.parquet(s"s3n://${s3config.getString("s3_bucket")}/staging_data/OL")
      .withColumn("inst_dt",current_date())
  }



  def writeRedshift(s3config: Config, inputDF: DataFrame, redshiftConfig: Config, table_name: String
                    ): Unit = {
    val jdbcUrl = Constants.getRedshiftJdbcUrl(redshiftConfig)

        inputDF.write
          .format("com.databricks.spark.redshift")
          .option("url", jdbcUrl)
          .option("tempdir", s"s3n://${s3config.getString("s3_bucket")}/temp")
          .option("forward_spark_s3_credentials", "true")
          .option("dbtable", s"$table_name")
          .option("tempFormat", "CSV GZIP")
          .mode(SaveMode.Overwrite)
          .save()

    }

}
