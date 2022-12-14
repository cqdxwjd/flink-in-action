package yunli.flink.streaming;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 自定义实现一个支持多并行度的Source
 */
public class MyParallelSource implements ParallelSourceFunction<Long> {
    private long count = 1L;
    private boolean isRunning = true;

    /**
     * 主要的方法
     * 启动一个Source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     *
     * @param ctx
     * @throws Exception
     */
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
}
