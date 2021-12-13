package yunli.flink.streaming.stock

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 实时计算某只股票的价格最大值
 *
 * @author wangjingdong
 * @date 2021/11/14 12:36
 * @Copyright © 云粒智慧 2018
 */
object MaxPrice {

  def main(args: Array[String]): Unit = {
    // 设置执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    // 每5秒生成一个Watermark
    environment.getConfig.setAutoWatermarkInterval(1000L)
    // 读取数据源
    val stream = environment.addSource(new StockSource("stock/trade-20200108.csv"))
      .assignTimestampsAndWatermarks(
        new AssignerWithPeriodicWatermarks[StockPrice] {
          var currentMaxTimestamp: Long = 0

          override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimestamp)

          override def extractTimestamp(element: StockPrice, previousElementTimestamp: Long): Long = {
            val eventTime = element.ts
            //            currentMaxTimestamp = Math.max(ts, currentMaxTimestamp)
            currentMaxTimestamp = eventTime
            eventTime
          }
        }
      )
    // 进行转换操作
    val res = stream
      .keyBy(_.symbol)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .max("price")
    // 结果输出
    res.print()
    // 执行
    environment.execute("max price")
  }
}
