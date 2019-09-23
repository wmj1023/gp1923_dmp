
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import util.tagUtils

object tagsContext2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("目录不正确")
      sys.exit()
    }

    val Array(inputPath, appPath) = args
    val spark = SparkSession.builder()
      .appName("Tags2")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    //    //调用HbaseApi
    //  val load = ConfigFactory.load()
    //    //获取表明
    //  val HbaseTableName: String = load.getString("HBASE.tableName")
    //    //创建Hadoop任务
    //val configuration= spark.sparkContext.hadoopConfiguration
    //    //配置Hbase连接
    //configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.host"))
    //
    //    //获取Connection 连接
    //val hbConn = ConnectionFactory.createConnection(configuration)
    //    val hbadmin = hbConn.getAdmin
    //    //判断当前表是否被使用
    //    if(!hbadmin.tableExists(TableName.valueOf(HbaseTableName))){
    //      println("当前表可用")
    //      //创建表对象
    //      val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
    //
    //
    //    //创建列簇
    //    val hColumnDescriptor = new HColumnDescriptor("tags")
    //
    //    //将创建好的列表加入表中
    //      tableDescriptor.addFamily(hColumnDescriptor)
    //      hbadmin.createTable(tableDescriptor)
    //      hbadmin.close()
    //      hbConn.close()
    //    }
    //    val conf = new JobConf(configuration)
    //
    //    //指定输出类型
    //    conf.setOutputFormat(classOf[TableOutputFormat])
    //
    //    //指定输出哪张表
    //    conf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)
    //读取数据文件
    val df = spark.read.parquet(inputPath)
    //读取字典文件
    val docsRDD = spark.sparkContext.textFile(appPath).map(_.split("\\s")).filter(_.length >= 5)
      .map(arr => (arr(4), arr(1))).collectAsMap()

    val broadcast = spark.sparkContext.broadcast(docsRDD)

    //获取所有ID
    val allUserId = df.rdd.map(row => {
      val strList = tagUtils.getallUserId(row)
      (strList, row)
    })

    //构建点集合
    val verties = allUserId.flatMap(row => {
      //获取所有数据
      val rows = row._2


      val adList = TagsAd.makeTags(rows)
      val appList = Tagsapp.makeTags(rows, broadcast.value)
      val areaList = TagsArea.makeTags(rows)
      val channelList = TagsChannel.makeTags(rows)
      val kwList = TagsKeyWords.makeTags(rows)
      val tagList = adList ++ appList ++ channelList ++ areaList ++ kwList
      val VD = row._1.map((_, 0)) ++ tagList
      row._1.map(uId => {
        if (row._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })

    //打印
    //verties.take(20).foreach(println)
    //构建边的集合
    val edges = allUserId.flatMap(row => {
      //A B C:A->B A->C
      row._1.map(uId => Edge(row._1.head.hashCode.toLong, uId.hashCode.toLong, 0))

    })
    //edges.foreach(println)
    //构建图
    val graph = Graph(verties, edges)
    // 根据图计算中的连通图算法，通过图中的分支，连通所有的点
    // 然后在根据所有点，找到内部最小的点，为当前的公共点
    val vertices = graph.connectedComponents().vertices
    // 聚合所有的标签
//    vertices.join(verties).foreach(println)
    vertices.join(verties).map({
      case (uid, (cnId, tagsAndUserId)) => {
        (cnId, tagsAndUserId)
      }
    }).reduceByKey(
      (list1,list2)=> {
        (list1 ++ list2)
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toList
      }).foreach(println)
        // .map{
//      case (userId,userTags) =>{
//        // 设置rowkey和列、列名
//        val put = new Put(Bytes.toBytes(userId))
//        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(day),Bytes.toBytes(userTags.mkString(",")))
//        (new ImmutableBytesWritable(),put)
//      }
//    }.saveAsHadoopDataset(conf)

    spark.stop()
  }
}
