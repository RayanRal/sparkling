package com.rayanral

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files

trait SparkStreamingTester extends SparkTester {

  val checkpointDir: File = Files.createTempDirectory("stream_checkpoint").toFile
  FileUtils.forceDeleteOnExit(checkpointDir)
  sparkSession.sparkContext.setCheckpointDir(checkpointDir.toString)

  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 9092,
    zooKeeperPort = 2181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "true")
  )

  EmbeddedKafka.start()(kafkaConfig)
}
