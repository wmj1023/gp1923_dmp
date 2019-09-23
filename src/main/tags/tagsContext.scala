import org.apache.spark.sql.SparkSession
import util.tagUtils

/**
  * 上下文标签主类
  */
object tagsContext {

  def main(args: Array[String]) = {

      if(args.length!=2){
        println("目录不正确")
        sys.exit()
      }
    val Array(inputPath,appPath)=args
    //创建上下文
    val sparkSession = SparkSession.builder().appName("context")
      .master("local").getOrCreate()

    val df = sparkSession.read.parquet(inputPath)
    import  sparkSession.implicits._

    //生成app映射（appid->appname）广播变量
    val docMap = sparkSession.sparkContext.textFile(appPath).map(_.split("\\s")).filter(_.length >= 5)
      .map(arr => (arr(4), arr(1))).collectAsMap()
    val appBroadcast = sparkSession.sparkContext.broadcast(docMap)



    //处理数据信息
    df.map(row=>{
      // 获取用户的唯一ID
      val userId = tagUtils.getOneUserId(row)
      // 接下来标签 实现
      val adList = TagsAd.makeTags(row)
      val appList = Tagsapp.makeTags(row,appBroadcast.value)
      val areaList = TagsArea.makeTags(row)
      val channelList = TagsChannel.makeTags(row)

      val kwList = TagsKeyWords.makeTags(row)
      // 商圈
      val businessList = BusinessTag.makeTags(row)
      (userId,areaList ++ adList ++ channelList ++ kwList ++ appList++businessList)
    }).rdd.reduceByKey((list1,list2)=>{
      (list1:::list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    }).foreach(println)
    }



}