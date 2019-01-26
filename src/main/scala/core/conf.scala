package core

import scala.xml.XML

object conf {
  def parseMysqlConf(): Map[String, String] = {
    val xml = XML.loadFile("/var/conf/mysql-conf.xml")
    val host = (xml \ "host").text
    val port = (xml \ "port").text
    val database = (xml \ "database").text
    val user = (xml \ "user").text
    val password = (xml \ "password").text
    Map("host" -> host, "port" -> port, "database" -> database, "user" -> user, "password" -> password)
  }
}