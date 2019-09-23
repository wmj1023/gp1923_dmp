import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import util.Tag

object TagsChannel extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row = args(0).asInstanceOf[Row]
    //获取渠道id
    val channelID: Int = row.getAs[Int]("adplatformproviderid")
    if (StringUtils.isNotBlank(channelID.toString)){
      list:+=("CN"+channelID,1)
    }
    list
  }
}
