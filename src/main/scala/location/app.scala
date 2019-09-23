package location

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import util.rptUtill


object app {
  def main(args: Array[String]): Unit = {
    if (args.length != 3){
      println("目标不正确，退出程序")
      sys exit()
    }
    val Array(inputPath,outputPath,data)= args
    val sparkSession = SparkSession
      .builder()
      .appName("媒体分析")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    //获取数据
    val df = sparkSession.read.parquet(inputPath)
    //获取字典数据
    val docMap = sparkSession.sparkContext.textFile(data).map(_.split("\\s")).filter(_.length >= 5)
      .map(arr => (arr(4), arr(1))).collectAsMap()

    //进行广播
    val broadcast = sparkSession.sparkContext.broadcast(docMap)

    //去媒体相关字段
    df.rdd.map(row=>{
      var appName = row.getAs[String]("appname")
      if(StringUtils.isBlank(appName)){
        appName = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val rptList: List[Double] = rptUtill.ReqPt(requestmode,processnode)
      //处理展示点击数
      val clickList = rptUtill.clickPt(requestmode,iseffective)
      //处理广告
      val adList: List[Double] = rptUtill.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      //所有指标
      val allList:List[Double] =  rptList ++ clickList ++ adList
      (appName,allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputPath)


  }
}
