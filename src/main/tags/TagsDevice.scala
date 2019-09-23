import org.apache.spark.sql.Row
import util.Tag

object TagsDevice extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
   val row = args(0).asInstanceOf[Row]
    val client = row.getAs[Int]("client")
    val ispname = row.getAs[String]("ispname")
    val networkmannername = row.getAs[String]("networkmannername")
    if(client ==1) {
      list :+= ("D00010001", 1)
    }else if(client ==2) {
      list :+= ("D00010002", 1)
    }else if(client == 3) {
      list :+= ("D0001003",1)
    }else {
      list :+= ("D0001004",1)
    }
    networkmannername.toUpperCase match {
      case "WIFI" => list:+= ("D00020001",1)
      case "4G" => list :+= ("D00020002",1)
      case "3G" => list :+= ("D00020003",1)
      case "2G" => list :+= ("D00020004",1)
      case _ => list :+= ("D00020005",1)
    }
    ispname.toUpperCase match {
      case "移动" => list:+= ("D00030001",1)
      case "联通" => list :+= ("D00030002",1)
      case "电信" => list :+= ("D00030003",1)
      case _ => list :+= ("D00030004",1)
    }
    list
  }
}
