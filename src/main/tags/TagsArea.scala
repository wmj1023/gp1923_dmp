import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import util.Tag

object TagsArea extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //获取数据类型
    val row: Row = args(0).asInstanceOf[Row]
    //获取所在省份的名称
    val pro: String = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    if (args.length > 0){
      if (StringUtils.isBlank(pro)) {
        list:+= ("ZP"+pro,1)
      }
      //获取所在城市的名称
      if (StringUtils.isNotBlank(cityname)){
        list:+=("ZC"+cityname,1)
      }
    }
    list
  }
}
