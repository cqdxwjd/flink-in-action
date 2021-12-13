package yunli.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 单词计数之滑动窗口计算
 */
public class SocketWindowWordCountJava {
    public static void main(String[] args) throws Exception {
        // 获取需要的端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port set. use default port 9999--Java");
            port = 9999;
        }
        // 获取Flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "172.25.5.11";
        char delimiter = '\n';
        // 连接Socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter, 0);
        DataStream<WordWithCount> windowCounts =
                text.flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> collector) throws Exception {
                        String[] split = value.split("\\s");
                        for (String word : split) {
                            collector.collect(new WordWithCount(word, 1L));
                        }
                    }
                }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1)).sum("count");
        // 把数据打印到控制台并且设置并行度
        windowCounts.print().setParallelism(1);
        // 这一行代码一定要实现，否则程序不执行
        env.execute("Socket window count");
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
