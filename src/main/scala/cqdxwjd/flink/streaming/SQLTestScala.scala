package cqdxwjd.flink.streaming

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sinks.CsvTableSink

object SQLTestScala {
  def main(args: Array[String]): Unit = {
    val bEnv = ExecutionEnvironment.getExecutionEnvironment
    val bTableEnv = TableEnvironment.getTableEnvironment(bEnv)
    // 隐式转换
    import org.apache.flink.api.scala._
    val dataSource = bEnv.readTextFile("C:\\Users\\shuihuo\\Desktop\\git\\flink-in-action\\data\\student.txt")
    val inputData = dataSource.map(line => {
      val splits = line.split(",")
      val stu = new Student(splits(0), splits(1).toInt)
      stu
    })
    // 将DataSet转换为Table
    val table = bTableEnv.fromDataSet(inputData)
    // 注册student表
    bTableEnv.registerTable("student", table)
    // 执行SQL查询
    val sqlQuery = bTableEnv.sqlQuery("select count(1),avg(age) from student")
    // 创建CsvTableSink
    val csvTableSink = new CsvTableSink("C:\\Users\\shuihuo\\Desktop\\git\\flink-in-action\\data\\result2.csv", ",", 1,
      FileSystem.WriteMode.OVERWRITE)
    // 注册TableSink
    bTableEnv.registerTableSink("csvOutputTable", Array[String]("count", "avg_age"), Array[TypeInformation[_]](Types
      .LONG, Types.INT), csvTableSink)
    sqlQuery.insertInto("csvOutputTable")
    bEnv.execute("SQL-Batch")
  }

  case class Student(name: String, age: Int)

}
