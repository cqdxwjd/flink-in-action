package yunli.flink.streaming.stock

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 实时计算某只股票的价格最大值
 *
 * @author wangjingdong
 * @date 2021/11/14 12:36
 * @Copyright © 云粒智慧 2018
 */
object MaxPrice2 {

  def main(args: Array[String]): Unit = {
    // 设置执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    environment.setParallelism(1)
    val stream = environment.addSource(new StockSource("stock/trade.csv"))
    // 进行转换操作
    val res = stream
      .keyBy(_.symbol)
      .timeWindow(Time.seconds(5))
      .max("price")
    // 结果输出
    res.print()
    // 执行
    environment.execute("max price")
  }
}
