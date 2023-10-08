import scala.io.Source
import scala.xml.XML

object CurrencyConverter {
  def getCurrencyRate(currencyCode: String): Option[Double] = {
    val url = s"https://www.cbr.ru/scripts/XML_daily.asp?date_req=01/10/2023"
    val xmlString = Source.fromURL(url, "windows-1251").mkString

    try {
      val parsedXml = XML.loadString(xmlString)
      val rateNode = (parsedXml \\ "Valute").find(v => (v \ "CharCode").text == currencyCode)
      
      rateNode match {
        case Some(rate) =>
          val nominal = (rate \ "Nominal").text.toInt
          val rateValue = (rate \ "Value").text.replace(",", ".").toDouble
          val oneRubToCurrency = rateValue / nominal
          Some(oneRubToCurrency)
        case None =>
          None
      }
    } catch {
      case e: Exception =>
        println(s"Error when getting the exchange rate: ${e.getMessage}")
        None
    }
  }
}
