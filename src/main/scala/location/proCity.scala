package location
import org.apache.spark.sql.{DataFrame, SparkSession}

object proCity {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("目标不正确，退出程序")
      sys.exit()
    }
    val Array(inputPath) =args
    val sparkSession = SparkSession.builder().appName("province city").master("local").config("spark.serializer","org.apache.spark.serializer.KryoSerializer").getOrCreate()
    val sc =sparkSession.sparkContext
    //获取数据
    val df = sparkSession.read.parquet(inputPath)
    //注册临时试图
    df.createTempView("log")
    val df2: DataFrame = sparkSession.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")
    df2.write.partitionBy("provincename","cityname").json("E:\\千峰\\spark\\项目\\项目day01\\Spark用户画像分析\\location")
    //存mysql
    //通过config配置文件依赖进行加载相关的配置信息
    //val load  = ConfigFactory.load()
    //创建properties对象
    //val prop = new Properties()
    //prop.setProperty("user",load.getString("jdbc.user"))
    //prop.setProperty("password",load.getString("jdbc.password"))
    //存储
    //df2.write.mode(SaveMode.Append).jdbc(
    //    load.getString("jdbc.url"),load.getString("jdbc.tablName"),prop) )
    sparkSession.stop()

  }

}
