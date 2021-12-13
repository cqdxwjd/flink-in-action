package yunli.flink.streaming.stock

import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.control.Breaks

/**
 * @author wangjingdong
 * @date 2021/11/14 11:47
 * @Copyright © 云粒智慧 2018
 */
class StockSource extends SourceFunction[StockPrice] {
  // source是否正在执行
  var isRunning = true
  // 数据集文件名
  var path: String = null
  var streamSource: InputStream = null

  def this(path: String) {
    this()
    this.path = path
  }

  override def run(ctx: SourceFunction.SourceContext[StockPrice]): Unit = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss")
    streamSource = this.getClass.getClassLoader.getResourceAsStream(path)
    val reader = new BufferedReader(new InputStreamReader(streamSource))
    var line: String = null
    var isFirstLine: Boolean = true
    var timeDiff: Long = 0
    var lastEventTs: Long = 0
    val breaks = new Breaks
    breaks.breakable(
      while (isRunning) {
        line = reader.readLine()
        if (Option(line).isEmpty) {
          breaks.break()
        }
        val itermStrArr = line.split(",")
        val dateTime = LocalDateTime.parse(itermStrArr(1) + " " + itermStrArr(2), formatter)
        val eventTs = Timestamp.valueOf(dateTime).getTime
        if (isFirstLine) {
          // 从第一行数据提取时间戳
          lastEventTs = eventTs
          isFirstLine = false
        }
        val stockPrice = StockPrice(itermStrArr(0), eventTs, itermStrArr(3).toDouble, itermStrArr(4).toInt)
        timeDiff = eventTs - lastEventTs
        if (timeDiff > 0) {
          Thread.sleep(timeDiff)
        }
        //        Thread.sleep(1000)
        ctx.collect(stockPrice)
        lastEventTs = eventTs
      }
    )
  }

  // 停止发送数据
  override def cancel(): Unit = {
    streamSource.close()
    isRunning = false
  }
}
