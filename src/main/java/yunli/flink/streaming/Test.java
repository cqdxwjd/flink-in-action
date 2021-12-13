package yunli.flink.streaming;

import org.apache.flink.api.scala.ExecutionEnvironment;

public class Test {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableForceAvro();
        env.getConfig().enableForceKryo();
//        env.getConfig().addDefaultKryoSerializer();
//        new DataStream();
    }
}
