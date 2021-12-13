package yunli.flink.doc.spendreport

import yunli.flink.doc.spendreport.FraudDetector.logger
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.{Alert, Transaction}
import org.openjdk.jol.info.GraphLayout
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author wangjingdong
 * @date 2021/11/20 14:45
 * @Copyright © 云粒智慧 2018
 */
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  @transient private var flagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _

  // count the number of record
  @transient private var countState: ValueState[java.lang.Long] = _

  override def processElement(transaction: Transaction,
                              context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
                              collector: Collector[Alert]): Unit = {
    // 单个对象占用的内存大小
    val layout = GraphLayout.parseInstance(transaction)
    val size = layout.totalSize()

    val printable = layout.toPrintable
    val footprint = layout.toFootprint

    var lastCount = countState.value()
    var newCount = 1L
    if (lastCount == null) {
      newCount = 0L
    } else {
      newCount = lastCount + 1
    }
    countState.update(newCount)
    // print the current processed record num
    if (newCount % 1000000 == 0) {
      logger.info(newCount.toString)
    }
    // get the current state for the current key
    val lastTransactionWasSmall = flagState.value()

    // check if the flag is set
    if (lastTransactionWasSmall != null) {
      if (transaction.getAmount > FraudDetector.LARGE_AMOUNT) {
        // output an alert downstream
        val alert = new Alert
        alert.setId(transaction.getAccountId)
        collector.collect(alert)
      }
      // clean up our state
      cleanUp(context)
    }

    if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
      // set the flag to ture
      flagState.update(true)

      // set the timer and timer state
      val timer = context.timerService().currentProcessingTime() + FraudDetector.ONE_SECONDS
      context.timerService().registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }
  }

  private def cleanUp(context: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
    // delete timer
    val timer = timerState.value()
    context.timerService().deleteProcessingTimeTimer(timer)

    // clean up all states
    timerState.clear()
    flagState.clear()
  }

  override def onTimer(timestamp: Long,
                       context: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
                       collector: Collector[Alert]): Unit = {
    // remove flag after 1 minute
    timerState.clear()
    flagState.clear()
  }

  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)

    val timerDescriptor = new ValueStateDescriptor[java.lang.Long]("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)

    val countDescriptor = new ValueStateDescriptor[java.lang.Long]("count-state", Types.LONG)
    countState = getRuntimeContext.getState(countDescriptor)
  }
}

object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long = 60 * 1000L
  val ONE_SECONDS: Long = 1000L
  val ONE_MILLIS: Long = 1L
  val TEN_MILLIS: Long = 10L
  val ONE_HUNDRED_MILLIS: Long = 100L

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
}
