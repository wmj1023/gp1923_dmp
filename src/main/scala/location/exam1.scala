package location

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object exam1 {
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
            val name = json.getString("type")
            name.split(";",-1).foreach(
              (item:String)=>result.append(item)
            )
          }
        }
      }
      result

    })
    res.foreach(println)
   val rdd1: RDD[(String, Int)] = res.flatMap(x=>x).map(x=>(x,1)).reduceByKey(_+_).sortByKey()
    rdd1.foreach(println)


  }
}
