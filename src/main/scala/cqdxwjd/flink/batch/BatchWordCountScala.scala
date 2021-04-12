package cqdxwjd.flink.batch

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    val inputPath = "C:\\Users\\shuihuo\\Desktop\\git\\flink-in-action\\data\\file"
    val outPath = "C:\\Users\\shuihuo\\Desktop\\git\\flink-in-action\\data\\result"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)
    val counts = text.flatMap(_.toLowerCase.split("\\W+")).filter(_.nonEmpty).map((_, 1)).groupBy(0).sum(1)
    counts.writeAsCsv(outPath, "\n", " ").setParallelism(1)
    env.execute("batch word count")
  }
}
