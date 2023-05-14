package com.rayanral

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, Suite}

import java.io.File
import java.nio.file.Files

trait SparkStreamingTester extends SparkTester with Suite with BeforeAndAfterEach {

  val checkpointDir: File = Files.createTempDirectory("stream_checkpoint").toFile
  FileUtils.forceDeleteOnExit(checkpointDir)
  sparkSession.sparkContext.setCheckpointDir(checkpointDir.toString)

  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 9092,
    zooKeeperPort = 2181,
    customBrokerProperties = Map("auto.create.topics.enable" -> "true")
  )

  override def beforeEach(): Unit = {
    EmbeddedKafka.start()(kafkaConfig)
  }

  override def afterEach(): Unit = {
    EmbeddedKafka.stop()
  }

}
