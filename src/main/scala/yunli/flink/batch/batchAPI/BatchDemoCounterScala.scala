package yunli.flink.batch.batchAPI

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object BatchDemoCounterScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val data = env.fromElements("a", "b", "c", "d")
    val res = data.map(new RichMapFunction[String, String] {
      val numLines = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        getRuntimeContext.addAccumulator("num-lines", this.numLines)
      }

      override def map(value: String): String = {
        this.numLines.add(1)
        value
      }

    }).setParallelism(4)
    res.writeAsText("C:\\Users\\shuihuo\\Desktop\\git\\flink-in-action\\data\\sum-scala")
    val jobResult: JobExecutionResult = env.execute("counter")
    val num = jobResult.getAccumulatorResult[Int]("num-lines")
    println(num)
  }
}
