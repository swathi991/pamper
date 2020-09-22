    package com.pg

    import com.pg.utils.{Constants, Utility}
    import com.typesafe.config.ConfigFactory
    import org.apache.spark.sql.SparkSession

    import scala.collection.JavaConversions._

    object TargetDataLoading {
      def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
          //.master("local[*]")
          .appName("Target Data Loading")
          .getOrCreate()
        spark.sparkContext.setLogLevel(Constants.ERROR)

        val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
        val s3Config = rootConfig.getConfig("s3_conf")
        val tgtList = rootConfig.getStringList("target_list").toList
        val redshiftConfig = rootConfig.getConfig("redshift_conf")

        spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
        spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))
        spark.udf.register("FN_UUID", () => java.util.UUID.randomUUID().toString())

        for (tgt <- tgtList) {
          val tgtConfig = rootConfig.getConfig(tgt)

          tgt match {
            case "REGIS_DIM" =>

              val regisData = Utility.read1CP(spark, s3Config)
             // regisData.createOrReplaceTempView("STG_1CP")
              regisData.createOrReplaceTempView("REGIS_DIM")

              // regisData.createOrReplaceTempView("DATAMART_REGIS_DIM")

              val regisDF = spark.sql(s"${tgtConfig.getString("loadingQuery")}").coalesce(1)
              regisDF.show()

              Utility.writeRedshift(s3Config, regisDF, redshiftConfig,
                s"${tgtConfig.getString("tableName")}")

            case "CHILD_DIM" =>

              val cdData = Utility.read1CP(spark, s3Config)
              cdData.createOrReplaceTempView("STG_1CP_child")

              val childData = spark.sql(s"${tgtConfig.getString("loadingQuery")}").coalesce(1)


              Utility.writeRedshift(s3Config, childData, redshiftConfig,
                s"${tgtConfig.getString("tableName")}")

            case "RTL_TXN_FACT" =>
              val sqlConfig = rootConfig.getConfig("SB")

              val OLstg = Utility.readOLStg(spark, s3Config)
              OLstg.createOrReplaceTempView("OL")

              //val stg1CP= Utility.reads3Data(spark,s3Config,s"staging_data/1CP")
              val stgSB = Utility.readSBStg(spark, s3Config)
              stgSB.createOrReplaceTempView("SB")

              val fctData = spark.sql(s"${tgtConfig.getString("loadingQuery")}").coalesce(1)
              Utility.writeRedshift(s3Config, fctData, redshiftConfig,
                s"${tgtConfig.getString("tableName")}")



            // StageDataLoading.read1CPStg.createOrReplaceTempView("DATAMART_REGIS_DIM")


          }

        }

      }
    }

