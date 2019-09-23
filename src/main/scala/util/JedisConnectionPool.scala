package util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/***
  * redis连接
  */
object JedisConnectionPool {
val config = new JedisPoolConfig()

  config.setMaxTotal(20)

  config.setMaxIdle(10)


  private val pool = new JedisPool(config,"192.168.91.10",6379,10000)
  def getConnection():Jedis={
    pool.getResource
  }
}
