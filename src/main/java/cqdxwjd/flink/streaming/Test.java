package cqdxwjd.flink.streaming;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.scala.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class Test {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableForceAvro();
        env.getConfig().enableForceKryo();
//        env.getConfig().addDefaultKryoSerializer();
//        new DataStream();
    }
}
