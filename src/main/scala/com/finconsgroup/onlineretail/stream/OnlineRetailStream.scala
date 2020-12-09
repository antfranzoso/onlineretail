package com.finconsgroup.onlineretail.stream

import java.time.{Duration, ZoneId}
import java.util.Properties

import com.finconsgroup.onlineretail.model.Invoice
import com.finconsgroup.onlineretail.utils.Constants
import com.finconsgroup.onlineretail.utils.serde.InvoiceJsonSerde
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}


object OnlineRetailStream {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("env.conf")
    val props = new Properties()
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(Constants.BOOTSTRAP_SERVER_IP))
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "online-retail-metrics")
    props.setProperty(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
      classOf[LogAndContinueExceptionHandler].getName)

    import Serdes._
    implicit val valueSerde: Serde[Invoice] = new InvoiceJsonSerde


    val builder = new StreamsBuilder
    import org.apache.kafka.streams.scala.ImplicitConversions._
    val source = builder.stream[String, Invoice](config.getString(Constants.ONLINE_RETAIL_TOPIC))

    val countryPerCustomer = source.map((_, value) => {
      val day = value.invoiceDate.toInstant.atZone(ZoneId.systemDefault()).toLocalDate
      s"${value.country}~${day.toEpochDay.toString}" -> 1L
    }).groupByKey.reduce(_ + _)

    val revPerDay = source.map((_, v) => {
      val day = v.invoiceDate.toInstant.atZone(ZoneId.systemDefault()).toLocalDate
      day.toEpochDay.toString -> v.items.foldLeft(0.0) { (acc, elem) => acc + elem.unitPrice * elem.quantity }
    }).groupByKey.reduce(_ + _)

    val topProducts = source.flatMap((_, v) => {
      val day = v.invoiceDate.toInstant.atZone(ZoneId.systemDefault()).toLocalDate
      v.items.map(it => s"{${day.toEpochDay.toString}~${it.stockCode}" -> it.quantity)
    }).groupByKey.reduce(_ + _)


    countryPerCustomer.toStream.to(config.getString(Constants.CUSTOMER_METRIC_TOPIC))
    revPerDay.toStream.to(config.getString(Constants.REV_METRIC_TOPIC))
    topProducts.toStream.to(config.getString(Constants.PRODUCTS_METRIC_TOPIC))
    val stream = new KafkaStreams(builder.build, props)
    stream.start()

    sys.ShutdownHookThread {
      stream.close(Duration.ofSeconds(10))
    }

  }
}
