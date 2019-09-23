import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import util.Tag

import scala.collection.mutable

object Tagsapp extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
   //获取数据类型
    val row = args(0).asInstanceOf[Row]
    //获取app名称
    val appmap = args(1).asInstanceOf[mutable.HashMap[String,String]]
    val appid = row.getAs[String]("appid")
    val appname = row.getAs[String]("appname")
    if(StringUtils.isNoneBlank(appname)){
      list:+=("APP"+appname,1)
    }else{
      list :+=("App"+appmap.getOrElse(appid,appid),1)
  }
    list
  }
}
