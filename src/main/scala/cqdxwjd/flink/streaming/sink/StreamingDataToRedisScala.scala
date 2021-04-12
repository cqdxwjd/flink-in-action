package cqdxwjd.flink.streaming.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object StreamingDataToRedisScala {
  def main(args: Array[String]): Unit = {
    // 获取Socket端口号
    val port = 9999
    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 链接Socket获取输入数据
    val text = env.socketTextStream("hadoop01", port, '\n')
    import org.apache.flink.api.scala._
    val l_wordsData = text.map(line => ("l_words_scala", line))
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop03").setPort(6379).build()
    val redisSink = new RedisSink[Tuple2[String, String]](conf, new MyRedisMapper)
    l_wordsData.addSink(redisSink)
    env.execute("StreamingDataToRedisScala")
  }

  class MyRedisMapper extends RedisMapper[Tuple2[String, String]] {
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.LPUSH)

    override def getKeyFromData(data: (String, String)): String = data._1

    override def getValueFromData(data: (String, String)): String = data._2
  }

}
