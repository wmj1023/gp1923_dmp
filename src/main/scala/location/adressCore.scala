package location

import org.apache.spark.sql.SparkSession
import util.rptUtill

object adressCore {
  def main(args: Array[String]): Unit = {
    if(args.length !=2 ){
      println("目标不正确，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath)=args
    val sparkSession = SparkSession
      .builder()
      .appName("adress count")
      .master("local[2]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    //获取数据
    val df = sparkSession.read.parquet(inputPath)
    df.rdd.map(row=>{
      //根据指标的字段获取结果
       val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      //处理请求数
val rptList = rptUtill.ReqPt(requestmode,processnode)
      //处理展示点击
      val clickList = rptUtill.clickPt(requestmode,iseffective)
      //处理广告
        val adList = rptUtill.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      //所有指标
       val allList:List[Double] =  rptList++clickList++adList
      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),allList)

    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputPath)
  }
}
