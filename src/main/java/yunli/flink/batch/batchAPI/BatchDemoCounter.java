package yunli.flink.batch.batchAPI;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

public class BatchDemoCounter {
    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("a", "b", "c", "d");
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            // 1.创建累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 2.注册累加器
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public String map(String value) throws Exception {
                this.numLines.add(1);
                return value;
            }
        }).setParallelism(8);
        result.writeAsText("C:\\Users\\shuihuo\\Desktop\\git\\flink-in-action\\data\\sum");
        JobExecutionResult jobExecutionResult = env.execute("counter");
        // 3.获取累加器
        int num = jobExecutionResult.getAccumulatorResult("num-lines");
        System.out.println("num: " + num);
    }
}
