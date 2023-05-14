package com.rayanral


import io.github.embeddedkafka.EmbeddedKafka
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.flatspec._
import org.scalatest.matchers.should.Matchers._

import java.nio.charset.StandardCharsets

class SimpleStreamingTest extends AnyFlatSpec with SparkStreamingTester {


  "spark" should "receive message from Kafka" in {
    val topic = "test_topic"
    val tableName = "test_table"
    val testMessage = "test_message"

    val df = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"localhost:${kafkaConfig.kafkaPort}")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val query = df.writeStream
      .format("memory")
      .queryName(tableName)
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .start()

    EmbeddedKafka.publishStringMessageToKafka(topic, testMessage)
    query.processAllAvailable()

    val results = sparkSession.sql(f"SELECT value FROM $tableName").collect()
    results.length should be(1)
    val messageBytes = results.head.getAs[Array[Byte]]("value")
    new String(messageBytes, StandardCharsets.UTF_8) should be(testMessage)
  }

}
