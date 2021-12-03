package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.EnrichedRide;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.training.solutions.ridecleansing.RideCleansingSolution;
import org.apache.flink.util.Collector;
import org.joda.time.Interval;
import org.joda.time.Minutes;

/**
 * @author wangjingdong
 * @date 2021/11/26 13:47
 * @Copyright © 云粒智慧 2018
 */
public class MaxRide {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<TaxiRide> rides = env.addSource(ExerciseBase.rideSourceOrTest(new TaxiRideGenerator()));
        final SingleOutputStreamOperator<Tuple2<Integer, Minutes>> minutesByStartCell = rides
                .filter(new RideCleansingSolution.NYCFilter())
                .flatMap(new FlatMapFunction<TaxiRide, Tuple2<Integer, Minutes>>() {
                    @Override
                    public void flatMap(TaxiRide taxiRide, Collector<Tuple2<Integer, Minutes>> out) throws Exception {
                        if (!taxiRide.isStart) {
                            EnrichedRide enrichedRide = new EnrichedRide(taxiRide);
                            Interval interval = new Interval(enrichedRide.startTime.toEpochMilli(), enrichedRide.endTime.toEpochMilli());
                            Minutes minutes = interval.toDuration().toStandardMinutes();
                            out.collect(new Tuple2<>(enrichedRide.startCell, minutes));
                        }
                    }
                });
        minutesByStartCell.keyBy(value -> value.f0)
                .maxBy(1)
                .print();
        env.execute("Find the max ride for each startCell!");
    }
}
