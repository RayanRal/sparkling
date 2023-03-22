package com.rayanral

import utils.serializer.{TestJsonDeserializer, TestJsonSerializer}

import io.github.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.flatspec._
import org.scalatest.matchers.should.Matchers._

class RedAcceleratorStreamingTest extends AnyFlatSpec with SparkStreamingTester {


  "accelerator" should "increase movement of red vehicles" in {
    val topic = "test_topic"
    val tableName = "test_table"
    val testData = WarhammerUnit("Orks", "Trukk", "Red", 12)

    implicit val serializer: Serializer[WarhammerUnit] =
      new TestJsonSerializer[WarhammerUnit]

    val decoderFn = (v: Array[Byte]) => {
      val deserializer: Deserializer[WarhammerUnit] =
        new TestJsonDeserializer[WarhammerUnit]
      deserializer.deserialize("", v)
    }

    val decoderUdf = udf(decoderFn)

    import sparkSession.implicits._

    val df = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"localhost:${kafkaConfig.kafkaPort}")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .withColumn("decoded", decoderUdf(col("value")))
      .select("decoded.*")
      .as[WarhammerUnit]

    val accelerator = new Accelerator(sparkSession)
    val resultDf = accelerator.redGoezFasta(df)

    val query = resultDf.writeStream
      .format("memory")
      .queryName(tableName)
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .start()

    EmbeddedKafka.publishToKafka(topic, testData)
    query.processAllAvailable()

    val results = sparkSession.sql(f"SELECT * FROM $tableName").as[WarhammerUnit].collect()
    results.length should be(1)
    val result = results.head
    result.name should be(testData.name)
    result.movement should be(17)
  }

}
