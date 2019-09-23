package util

import com.alibaba.fastjson.{JSON, JSONObject}


/**
  * 解析经纬度
  */
object AmapUtil {
def getBusinessFromAmap(long:Double,lat:Double):String={
val location = long+","+lat
  //获取url
  val url = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=40b78b9819eead3f64dd6650bded7f2d"
  //调用Http接口发送请求
  val jsonstr = HttpUtil.get(url)
  //解析json串
  val jsonObject: JSONObject = JSON.parseObject(jsonstr)
  //判断该状态是否为1
  val status: Int = jsonObject.getIntValue("status")
  if(status == 0) return""
  //如果不为空
  val jSONObject = jsonObject.getJSONObject("regeocode")
  if(jSONObject == null) return ""
  val jsonObject2 = jSONObject.getJSONObject("addressComponent")
  if(jsonObject2 == null) return ""
  val jSONArray = jsonObject2.getJSONArray("businessAreas")
  if(jSONArray == null) return  ""
  // 定义集合取值
  val result = collection.mutable.ListBuffer[String]()
  // 循环数组
  for (item <- jSONArray.toArray()){
    if(item.isInstanceOf[JSONObject]){
      val json = item.asInstanceOf[JSONObject]
      val name = json.getString("name")
      result.append(name)
    }
  }
  // 商圈名字
  result.mkString(",")
}
}
