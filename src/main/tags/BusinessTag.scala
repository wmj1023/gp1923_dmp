import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import util.{AmapUtil, JedisConnectionPool, String2Type, Tag}

object BusinessTag extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
   val row =args(0).asInstanceOf[Row]
    var list = List[(String,Int)]()

    // 获取经纬度
//    val latString: String = row.getAs[String]("lat")
//    val longString: String = row.getAs[String]("long")
    // 新数据经纬度反了
    val longString: String = row.getAs[String]("lat")
    val latString: String = row.getAs[String]("long")

    val lat: Double = String2Type.toDouble(latString)
    val long: Double = String2Type.toDouble(longString)

    // 过滤经纬度
    if (lat >= 3.0 && lat <= 54.0 && long >= 73.0 && long <= 135.0) {
      // 先去数据库获取商圈
      val business: String = getBusiness(long, lat)
      // 判断缓存中是否有此商圈
      if (StringUtils.isNoneBlank(business)) {
        val lines: Array[String] = business.split(",")
        lines.foreach(f => list :+= (f, 1))
      }
    }


    //获取经纬度
//    if(String2Type.toDouble(row.getAs[String]("long")) >=73.0
//    && String2Type.toDouble(row.getAs[String]("long")) <=136.0
//      && String2Type.toDouble(row.getAs[String]("lat")) >=3.0
//      && String2Type.toDouble(row.getAs[String]("lat")) <=53.0
//    ){
//      val long = row.getAs[String]("long").toDouble
//      val lat = row.getAs[String]("lat").toDouble
//      //获取到商圈名称
//      val business = getBusiness(long,lat)
//      if (StringUtils.isNoneBlank(business)){
//        val str = business.split(",")
//        str.foreach(str=>{
//          list:+=(str,1)
//        })
//      }
//    }
list



  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long: Double, lat: Double):String = {
    //GeoHash码
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)
    //数据库查询当前商圈信息
    var business = redis_queryBusiness(geoHash)
    //去高德请求
    if(business == null){
      business=AmapUtil.getBusinessFromAmap(long,lat)
      //将高德获取的商圈存储数据库
      if(business != null && business.length >0){
        redis_insertBusiness(geoHash,business)
      }
    }
    business
  }

  /** 数据库获取商圈信息
    * @param geohash
    * @return
  */
  def redis_queryBusiness(geohash:String):String={
    val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  /**
    * 将商圈保存数据库
    */
  def redis_insertBusiness(geohash:String,business:String): Unit ={
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }
}
