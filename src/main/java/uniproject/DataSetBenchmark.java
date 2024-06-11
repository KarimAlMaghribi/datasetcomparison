package uniproject;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class DataSetBenchmark {

    private List<Tuple3<Integer, Integer, Long>> data;

    @Setup
    public void setUp() {
        data = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            data.add(new Tuple3<>(i, i % 100, System.currentTimeMillis()));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testDataSetFilter() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, Integer, Long>> dataSet = env.fromCollection(data);

        DataSet<Tuple3<Integer, Integer, Long>> result = dataSet.filter(new FilterFunction<Tuple3<Integer, Integer, Long>>() {
            @Override
            public boolean filter(Tuple3<Integer, Integer, Long> value) {
                return value.f0 % 2 == 0;
            }
        });

        result.collect();
    }

}
