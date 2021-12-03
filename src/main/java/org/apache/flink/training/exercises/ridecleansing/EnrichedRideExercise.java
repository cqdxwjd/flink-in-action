package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.EnrichedRide;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.training.solutions.ridecleansing.RideCleansingSolution;
import org.apache.flink.util.Collector;

/**
 * @author wangjingdong
 * @date 2021/11/25 16:46
 * @Copyright © 云粒智慧 2018
 */
public class EnrichedRideExercise {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<TaxiRide> rides = env.addSource(ExerciseBase.rideSourceOrTest(new TaxiRideGenerator()));
        SingleOutputStreamOperator<EnrichedRide> enrichedNYCRides = rides
                .filter(new RideCleansingSolution.NYCFilter())
                .map(new MapFunction<TaxiRide, EnrichedRide>() {
                    @Override
                    public EnrichedRide map(TaxiRide ride) throws Exception {
                        return new EnrichedRide(ride);
                    }
                });
        SingleOutputStreamOperator<EnrichedRide> enrichedNYCRides2 = rides.flatMap(new FlatMapFunction<TaxiRide, EnrichedRide>() {
            @Override
            public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
                RideCleansingSolution.NYCFilter filter = new RideCleansingSolution.NYCFilter();
                if (filter.filter(taxiRide)) {
                    out.collect(new EnrichedRide(taxiRide));
                }
            }
        });
        enrichedNYCRides.print();
        enrichedNYCRides2.print();
        env.execute("EnrichedRideExercise");
    }
}
