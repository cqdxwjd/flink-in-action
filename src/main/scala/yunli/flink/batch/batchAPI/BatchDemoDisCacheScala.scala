package yunli.flink.batch.batchAPI

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object BatchDemoDisCacheScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // 1.注册文件
    env.registerCachedFile("C:\\Users\\shuihuo\\Desktop\\git\\flink-in-action\\data\\a.txt", "b.txt")
    val data = env.fromElements("a", "b", "C")
    val result = data.map(new RichMapFunction[String, String] {
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val myFile = getRuntimeContext.getDistributedCache.getFile("b.txt")
        val lines = FileUtils.readLines(myFile)
        val it = lines.iterator()
        while (it.hasNext) {
          val line = it.next()
          println(line)
        }
      }

      override def map(value: String): String = {
        value
      }
    })
    result.print()
  }
}
