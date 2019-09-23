import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import util.Tag

object TagsKeyWords extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    //获取关键字
    val keywords = row.getAs[String]("keywords")
    if (StringUtils.isNotBlank(keywords)){
      val fields: Array[String] = keywords.split("\\|")
      for (word <-fields){
        if (word.length >= 3 && word .length <= 8){
          list:+=("K".concat(word),1)
        }
      }
    }
    list
  }
}
