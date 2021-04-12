package cqdxwjd.flink.streaming;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class TableAPITest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);
        // 创建一个TableSource
        TableSource<Row> csvSource = new CsvTableSource("C:\\Users\\shuihuo\\Desktop\\git\\flink-in-action\\data\\abc.csv"
                , new String[]{"name", "age"}, new TypeInformation[]{Types.STRING(), Types.INT()});
        // 注册一个TableSource，成为CsvTable
        tableEnvironment.registerTableSource("CsvTable", csvSource);
        Table csvTable = tableEnvironment.scan("CsvTable");
        Table csvResult = csvTable.select("name,age");
        DataStream<Student> csvStream = tableEnvironment.toAppendStream(csvResult, Student.class);
        csvStream.print().setParallelism(1);
        env.execute("csvStream");
    }

    public static class Student {
        public String name;
        public int age;

        public Student() {

        }

        public Student(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
