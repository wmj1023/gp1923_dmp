package location

import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SparkSession}

object adress {
  def main(args: Array[String]): Unit = {
    if(args.length !=1 ){
      println("目标不正确，退出程序")
      sys.exit()
    }
    val Array(inputPath)=args
    val sparkSession = SparkSession.builder().appName("adress count").master("local[2]").getOrCreate()
    val sc = sparkSession.sparkContext
    val df = sparkSession.read.parquet(inputPath)
    import  sparkSession.implicits._
    import org.apache.spark.sql.functions._
    df.select($"provincename",$"cityname",
      when($"REQUESTMODE"=== 1 and $"PROCESSNODE" >=1,value = 1).as("yuanshi"),
        when($"REQUESTMODE"=== 1 and $"PROCESSNODE" >=2,value = 1).as("youxiao"),
        when($"REQUESTMODE"=== 1 and $"PROCESSNODE" ===3,value = 1).as("guanggaoqingqiu"),
        when($"ISEFFECTIVE"=== 1 and $"ISBILLING" ===1 and $"ISBID"===1,value = 1).as("joincount"),
        when($"ISEFFECTIVE"===1 and $"ISBILLING"===1 and $"ISWIN"===1 and $"ADORDERID"=!=0,value = 1).as("jingjiacount"),

        when($"REQUESTMODE"=== 2 and $"ISEFFECTIVE" ===1,value = 1).as("yuanshi"),
        when($"REQUESTMODE"=== 3 and $"PROCESSNODE" ===1,value = 1).as("yuanshi"),

        when($"ISEFFECTIVE"=== 1 and $"ISBILLING" ===1 and $"ISWIN"===1,value = 1).as("yuanshi"),
        when($"ISEFFECTIVE"=== 1 and $"ISBILLING" ===1 and $"ISWIN"===1,value = 1).as("yuanshi")

    ).groupBy($"")
//    df.createTempView("adress")
//    val frame: DataFrame = sparkSession.sql(
//      """
//select provincename,cityname,
//sum(case when REQUESTMODE=1 and PROCESSNODE >=1 then 1 else 0 end) as yuanshi,
//sum(case when REQUESTMODE=1 and PROCESSNODE >=2 then 1 else 0 end) as youxiao,
//sum(case when REQUESTMODE=1 and PROCESSNODE =3 then 1 else 0 end) as guanggaoqingqiu,
//sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISBID=1 then 1 else 0 end) as joincount,
//sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISWIN=1 and ADORDERID!=0 then 1 else 0 end) as jingjiacount,
//sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISBID=1 then 1 else 0 end) / sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISWIN=1 and ADORDERID!=0 then 1 else 0 end) as jingjiacount,
//sum(case when REQUESTMODE=2 and ISEFFECTIVE=1 then 1 else 0 end)as showcount,
//sum(case when REQUESTMODE=3 and ISEFFECTIVE=1 then 1 else 0 end)as pointcount,
//sum(case when REQUESTMODE=3 and ISEFFECTIVE=1 then 1 else 0 end) / sum(case when REQUESTMODE=2 and ISEFFECTIVE=1 then 1 else 0 end)as showcount,
//sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISWIN=1 then 1 else 0 end)as dspXiaoFei,
//sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISWIN=1 then 1 else 0 end)as dspChengBen
//from adress
//group by provincename,cityname
//with rollup
//order by provincename,cityname
//    """.stripMargin)

//    frame.show()




    sparkSession.stop()
  }
}
