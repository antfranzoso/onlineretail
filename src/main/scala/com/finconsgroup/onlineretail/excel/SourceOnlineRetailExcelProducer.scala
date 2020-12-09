package com.finconsgroup.onlineretail.excel

import java.io.File
import java.nio.file.Paths
import java.util.Properties

import com.finconsgroup.onlineretail.model.Invoice
import com.finconsgroup.onlineretail.utils.Constants
import com.finconsgroup.onlineretail.utils.serde.InvoiceJsonSerializer
import com.github.pjfanning.xlsx.StreamingReader
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._

object SourceOnlineRetailExcelProducer {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("env.conf")
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(Constants.BOOTSTRAP_SERVER_IP))
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[InvoiceJsonSerializer].getName)

    val sourceFile = new File(Paths.get(config.getString(Constants.SOURCE_PATH)
      , config.getString(Constants.FILE_NAME)).toString)
    val wbook = StreamingReader.builder().rowCacheSize(1000).bufferSize(4096).open(sourceFile)

    val kafkaProducer = new KafkaProducer[String, Invoice](props)

    wbook.getSheetAt(0).rowIterator().asScala.toSeq.tail
      .map(r => r.getCell(0).getStringCellValue -> Invoice(r)).groupBy(_._1).foreach(elem => {
      val value = elem._2.foldLeft(Option.empty[Invoice])((acc, elem) => Option(Invoice.merge(elem._2, acc)))
      val record =
        new ProducerRecord(config.getString(Constants.ONLINE_RETAIL_TOPIC), elem._1, value.get)
      kafkaProducer.send(record)
    })

    kafkaProducer.close()
    wbook.close()
  }
}
