package com.finconsgroup.onlineretail.model

import java.util.{Calendar, Date, GregorianCalendar}

import org.apache.poi.ss.usermodel.Row

import scala.util.Try

case class Invoice(invoiceNo: String, items: Seq[Item], customerId: Int, country: String, invoiceDate: Date) {
  override def toString: String = s"{$invoiceNo,${items.toString()},$customerId, $country, $invoiceDate}"
}

object Invoice {

  final val dummyDate = new GregorianCalendar(1900, Calendar.JANUARY, 1).getTime

  def apply(row: Row): Invoice = new Invoice(
    Try(row.getCell(0).getStringCellValue).getOrElse("-1"),
    Seq(Item(
      Try(row.getCell(1).getStringCellValue).getOrElse("-1"),
      Try(row.getCell(2).getStringCellValue).getOrElse("-1"),
      Try(row.getCell(3).getNumericCellValue.toInt).getOrElse(-1),
      Try(row.getCell(3).getNumericCellValue).getOrElse(0)
    )),
    Try(row.getCell(6).getNumericCellValue.toInt).getOrElse(-1),
    Try(row.getCell(7).getStringCellValue).getOrElse("-1"),
    Try(row.getCell(4).getDateCellValue).getOrElse(dummyDate)
  )


  def merge(elem: Invoice, theOther: Option[Invoice]): Invoice =
    theOther.fold(elem) { e => elem.copy(items = e.items ++ elem.items) }

}

case class Item(stockCode: String, description: String, quantity: Int, unitPrice: Double)

