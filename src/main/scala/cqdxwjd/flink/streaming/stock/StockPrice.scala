package cqdxwjd.flink.streaming.stock

/**
 * @author wangjingdong
 * @date 2021/11/14 11:51
 * @Copyright © 云粒智慧 2018
 */
case class StockPrice(symbol: String, ts: Long, price: Double, volume: Int)
