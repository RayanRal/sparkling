package com.rayanral
package utils.serializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.Deserializer

import scala.reflect.ClassTag

class TestJsonDeserializer[T](implicit tag: ClassTag[T])
  extends Deserializer[T] {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def deserialize(topic: String, bytes: Array[Byte]): T =
    mapper.readValue(bytes, tag.runtimeClass.asInstanceOf[Class[T]])
}