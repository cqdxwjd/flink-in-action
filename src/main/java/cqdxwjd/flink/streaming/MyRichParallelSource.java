package cqdxwjd.flink.streaming;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义实现一个支持多并行度的Source
 * RichParallelSourceFunction会额外提供open和close方法
 * 如果在source中需要获取其他链接资源，那么可以在open方法中打开链接资源，在close中关闭链接资源
 */
public class MyRichParallelSource extends RichParallelSourceFunction<Long> {
    private long count = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open.......");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
