//package cqdxwjd.flink.streaming;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.operators.DataSource;
//import org.apache.flink.api.scala.typeutils.Types;
//import org.apache.flink.core.fs.FileSystem;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.api.java.BatchTableEnvironment;
//import org.apache.flink.table.sinks.CsvTableSink;
//
//public class SQLTest {
//    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment bTableEnv = TableEnvironment.getTableEnvironment(bEnv);
//        DataSource<String> dataSource = bEnv.readTextFile("C:\\Users\\shuihuo\\Desktop\\git\\flink-in-action\\data" +
//                "\\student.txt");
//        DataSet<Student> inputData = dataSource.map(new MapFunction<String, Student>() {
//
//            @Override
//            public Student map(String value) throws Exception {
//                String[] split = value.split(",");
//                return new Student(split[0], Integer.parseInt(split[1]));
//            }
//        });
//        // 将DataSet转换为Table
//        Table table = bTableEnv.fromDataSet(inputData);
//        // 注册Student表
//        bTableEnv.registerTable("student", table);
//        // 执行sql查询
//        Table sql = bTableEnv.sqlQuery("select count(1),avg(age) from student");
//        // 创建CsvTableSink
//        CsvTableSink csvTableSink = new CsvTableSink("C:\\Users\\shuihuo\\Desktop\\git\\flink-in-action\\data\\result.csv", ",", 1,
//                FileSystem.WriteMode.OVERWRITE);
//        // 注册TableSink
//        bTableEnv.registerTableSink("csvOutputTable", new String[]{"count", "avg_age"},
//                new TypeInformation[]{Types.LONG(), Types.INT()}, csvTableSink);
//        // 把结果数据添加到CsvTableSink中
//        sql.insertInto("csvOutputTable");
//        bEnv.execute("SQL-Batch");
//    }
//
//    public static class Student {
//        public String name;
//        public int age;
//
//        public Student() {
//        }
//
//        public Student(String name, int age) {
//            this.name = name;
//            this.age = age;
//        }
//
//        @Override
//        public String toString() {
//            return "Student{" +
//                    "name='" + name + '\'' +
//                    ", age=" + age +
//                    '}';
//        }
//    }
//}
