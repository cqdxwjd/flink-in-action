package cqdxwjd.flink.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @author wangjingdong
 * @date 2021/11/13 14:26
 * @Copyright © 云粒智慧 2018
 */
object TransformationTest {
  def main(args: Array[String]): Unit = {
    // 设置执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    var stream = env.fromElements(1, 2, -3, 0, 5, -9, 8);

    val res = stream.filter(e => e > 0)

    res.print()

    env.execute("filter")
  }
}
