package com.finconsgroup.onlineretail.model

import java.time.LocalDate

import com.finconsgroup.onlineretail.utils.serde.Json
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

case class CustomersMetric(country: String, date: LocalDate, count: Long)

class CustomerMetricJsonSerializer extends Serializer[CustomersMetric] {
  override def serialize(topic: String, data: CustomersMetric): Array[Byte] = Json.ByteArray.encode(data)
}

class CustomerMetricJsonDeserializer extends Deserializer[CustomersMetric] {
  override def deserialize(topic: String, data: Array[Byte]): CustomersMetric = {
    if (data == null) {
      null
    } else {
      Json.ByteArray.decode[CustomersMetric](data)
    }
  }
}

class CustomerMetricSerde extends Serde[CustomersMetric] {
  override def deserializer(): Deserializer[CustomersMetric] = new CustomerMetricJsonDeserializer

  override def serializer(): Serializer[CustomersMetric] = new CustomerMetricJsonSerializer
}