package cqdxwjd.flink.streaming

import cqdxwjd.flink.streaming.SocketWindowWordCountScala.WordWithCount
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.windowing.time.Time

import java.util.Properties

/**
 * @author wangjingdong
 * @date 2021/11/12 17:09
 * @Copyright © 云粒智慧 2018
 */
object WordCountKafkaInStdOut {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // kafka参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "heifen-227:9092,heifen-229:9092,heifen-231:9092")
    properties.setProperty("group.id", "flink-group")
    val inputTopic = "test"

    // source
    import org.apache.flink.api.scala._
    val consumer = new FlinkKafkaConsumer011[String](inputTopic, new SimpleStringSchema(), properties)
    val stream = env.addSource(consumer)

    // transformation
    val wordWithCount = stream.flatMap((line => line.split("[^a-zA-Z0-9]")))
      .map(word => WordWithCount(word, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    // sink
    wordWithCount.print()

    // execute
    env.execute("kafka streaming word count")
  }
}
