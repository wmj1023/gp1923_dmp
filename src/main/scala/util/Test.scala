package util

import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()
    import sparkSession.implicits._
      val df = sparkSession.read.parquet("D\\Group08")
    df.map(row=>{
      //圈
      AmapUtil.getBusinessFromAmap(
        String2Type.toDouble(row.getAs[String]("long")),
        String2Type.toDouble(row.getAs[String]("lat"))

      )
    }).rdd.foreach(println)
  }
}
