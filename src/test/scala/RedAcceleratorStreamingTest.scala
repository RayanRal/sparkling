package com.rayanral

import io.github.embeddedkafka.EmbeddedKafka
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.flatspec._
import org.scalatest.matchers.should.Matchers._

class RedAcceleratorStreamingTest extends AnyFlatSpec with SparkStreamingTester {

  import sparkSession.implicits._

  "accelerator" should "increase movement of red vehicles" in {
    val topic = "test_topic"
    val tableName = "test_table"
    val testData = Seq(
      WarhammerUnit("Orks", "Trukk", "Red", 12),
      WarhammerUnit("Orks", "Trukk", "Blue", 12),
      WarhammerUnit("Blood Angels", "Rhino", "Red", 12),
      WarhammerUnit("Adeptus Astartes", "Librarian", "Ultramarine", 6),
    )

    import io.github.embeddedkafka.Codecs.stringSerializer

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

    EmbeddedKafka.publishToKafka(topic, "4")
    query.processAllAvailable()

    val result = sparkSession.sql(f"SELECT * FROM $tableName").collect()
    assert(!result.isEmpty)
  }

}
