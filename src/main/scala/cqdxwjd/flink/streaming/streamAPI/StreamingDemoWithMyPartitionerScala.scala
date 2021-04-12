package cqdxwjd.flink.streaming.streamAPI

import cqdxwjd.flink.streaming.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingDemoWithMyPartitionerScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    import org.apache.flink.api.scala._
    val text = env.addSource(new MyNoParallelSourceScala)
    val tupleData = text.map(line => Tuple1(line))
    val partitionData = tupleData.partitionCustom(new MyPartitionerScala, 0)
    val result = partitionData.map(line => {
      println("当前线程id: " + Thread.currentThread().getId + ", value: " + line)
      line._1
    })
    result.print().setParallelism(1)
    env.execute("StreamingDemoWithMyPartitionerScala")
  }
}
