package com.rayanral

import org.apache.spark.sql._

trait SparkTester {

    val sparkSession: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("test spark app")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    }
}
