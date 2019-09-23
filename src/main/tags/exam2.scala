import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import util.Tag

object exam2 extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row = args(0).asInstanceOf[Row]
    //获取广告位类型和名称
    val adType = row.getAs[Int]("type")

    //广告名称
    val adName: String = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(adName)){
      list:+=("LN"+adName,1)
    }
    list

  }
}
