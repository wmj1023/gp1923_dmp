package location

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ListBuffer



object exam {
  //json数据归纳格式（考虑获取到数据的成功因素 status=1成功 starts=0 失败）：
  //   1、按照pois，分类businessarea，并统计每个businessarea的总数。
  //   2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("exam")
      .master("local")
      .getOrCreate()

    val df: RDD[String] = sparkSession.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\json.txt")


    val res: RDD[ListBuffer[String]] = df.map(x => {
      val result: ListBuffer[String] = collection.mutable.ListBuffer[String]()
      val jsonObject = JSON.parseObject(x)
      val status: Int = jsonObject.getIntValue("status")
      if (status == 1) {

        val jSONObject: JSONObject = jsonObject.getJSONObject("regeocode")

        val jSONArray: JSONArray = jSONObject.getJSONArray("pois")


        for (item <- jSONArray.toArray()) {
          if (item.isInstanceOf[JSONObject]) {
            val json = item.asInstanceOf[JSONObject]
            val name = json.getString("businessarea")
            result.append(name)
          }
        }
      }
      result

    })
        val rdd1: RDD[(String, Int)] = res.flatMap(x=>x).groupBy(y=>y).mapValues(_.size)
        rdd1.foreach(println)
    //  }

  }
}
