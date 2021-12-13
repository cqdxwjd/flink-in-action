package yunli.flink.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 单词计数之滑动窗口计算
 */
object SocketWindowWordCountScala {
  def main(args: Array[String]): Unit = {
    // 获取Socket端口号
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port set. use default port 9999--Scala")
        9999
      }
    }
    // 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 连接Socket获取输入数据
    val text = env.socketTextStream("172.25.5.11", port, '\n')
    // 解析数据
    import org.apache.flink.api.scala._
    val windowCounts = text.flatMap(line => line.split("\\s")).map(w => WordWithCount(w, 1L)).keyBy("word").timeWindow(Time.seconds(2),
      Time.seconds(1)).sum("count")
    windowCounts.print().setParallelism(1)
    // 执行任务
    env.execute("Socket window count")
  }

  case class WordWithCount(word: String, count: Long)
}

