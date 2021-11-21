//package cqdxwjd.flink.streaming
//
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.scala.typeutils.Types
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.table.api.TableEnvironment
//import org.apache.flink.table.sources.CsvTableSource
//
//object TableTestScala {
//  def main(args: Array[String]): Unit = {
//    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
//    val sTableEnv = TableEnvironment.getTableEnvironment(sEnv)
//    // 隐式转换
//    import org.apache.flink.api.scala._
//    // 创建一个TableSource
//    val csvSource = new CsvTableSource("C:\\Users\\shuihuo\\Desktop\\git\\flink-in-action\\data\\abc.csv",
//      Array[String]("name", "age"), Array[TypeInformation[_]](Types.STRING, Types.INT))
//    // 注册一个TableSource，称为CsvTable
//    sTableEnv.registerTableSource("CsvTable", csvSource)
//    val csvTable = sTableEnv.scan("CsvTable")
//    val csvResult = csvTable.select("name,age")
//    val csvStream = sTableEnv.toAppendStream[Student](csvResult)
//    csvStream.print().setParallelism(1)
//    sEnv.execute("csvStream")
//  }
//
//  case class Student(name: String, age: Int)
//
//}
