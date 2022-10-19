package com.rayanral

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object WhApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("spark://127.0.0.1:7077")
      .config(new SparkConf().setJars(Seq("./target/scala-2.12/sparkling_2.12-0.1.0-SNAPSHOT.jar")))
      .appName("Warhammer 40k").getOrCreate()
    import spark.implicits._

    val testData = Seq(
      WarhammerUnit("Orks", "Trukk", "Red", 12),
      WarhammerUnit("Orks", "Trukk", "Blue", 12),
      WarhammerUnit("Blood Angels", "Rhino", "Red", 12),
      WarhammerUnit("Adeptus Astartes", "Librarian", "Ultramarine", 6),
    )
    val testDf = spark.createDataset(testData)
    new Accelerator(spark).redGoezFasta(testDf).show()
  }

}
