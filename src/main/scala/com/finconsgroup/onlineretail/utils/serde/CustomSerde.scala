package com.finconsgroup.onlineretail.utils.serde

import java.lang.reflect.ParameterizedType
import java.lang.reflect.{ParameterizedType, Type}
import java.util

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.exc.{UnrecognizedPropertyException => UPE}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.finconsgroup.onlineretail.model.Invoice
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}



  object Json {

    type ParseException = JsonParseException
    type UnrecognizedPropertyException = UPE

    private val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

    private def typeReference[T: Manifest] = new TypeReference[T] {
      override def getType = typeFromManifest(manifest[T])
    }

    private def typeFromManifest(m: Manifest[_]): Type = {
      if (m.typeArguments.isEmpty) {
        m.runtimeClass
      }
      else new ParameterizedType {
        def getRawType = m.runtimeClass

        def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray

        def getOwnerType = null
      }
    }

    object ByteArray {
      def encode(value: Any): Array[Byte] = mapper.writeValueAsBytes(value)

      def decode[T: Manifest](value: Array[Byte]): T =
        mapper.readValue(value, typeReference[T])
    }

  }

  /**
    * JSON serializer for JSON serde
    *
    */
  class InvoiceJsonSerializer extends Serializer[Invoice] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

    override def serialize(topic: String, data: Invoice): Array[Byte] =
      Json.ByteArray.encode(data)

    override def close(): Unit = ()
  }

  /**
    * JSON deserializer for JSON serde
    *
    */
  class InvoiceJsonDeserializer extends Deserializer[Invoice] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

    override def close(): Unit = ()

    override def deserialize(topic: String, data: Array[Byte]): Invoice = {
      if (data == null) {
        return null
      } else {
        Json.ByteArray.decode[Invoice](data)
      }
    }
  }

  /**
    * JSON serde for local state serialization
    *
    */
  class InvoiceJsonSerde extends Serde[Invoice] {
    override def deserializer(): Deserializer[Invoice] = new InvoiceJsonDeserializer

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

    override def close(): Unit = ()

    override def serializer(): Serializer[Invoice] = new InvoiceJsonSerializer
}
