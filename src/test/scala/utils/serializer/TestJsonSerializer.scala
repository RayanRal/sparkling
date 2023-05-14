package com.rayanral
package utils.serializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

import scala.util.control.NonFatal

/**
 * Inspired by `org.apache.kafka.connect.json.JsonSerializer`
 */
class TestJsonSerializer[T] extends Serializer[T] {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def serialize(topic: String, data: T): Array[Byte] =
    mapper.writeValueAsBytes(data)
}