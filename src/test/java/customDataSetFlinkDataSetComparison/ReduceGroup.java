package customDataSetFlinkDataSetComparison;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import uniproject.CustomDataSet;
import uniproject.CustomDataSetImpl;

import java.util.Arrays;
import java.util.List;

public class ReduceGroup {

    @Test
    public void testDifferenceFunction() throws Exception {
        List<Tuple2<String, Integer>> data = Arrays.asList(
                Tuple2.of("A", 1),
                Tuple2.of("B", 1),
                Tuple2.of("C", 1),
                Tuple2.of("B", 2),
                Tuple2.of("C", 2),
                Tuple2.of("D", 2)
        );

        CustomDataSetImpl<Tuple2<String, Integer>> customDataSet = new CustomDataSetImpl<>(data);

        GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> reducer = new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (Tuple2<String, Integer> value : values) {
                    out.collect(value);
                }
            }
        };

        CustomDataSet<Tuple2<String, Integer>> result = customDataSet.reduceGroup(reducer);

        List<Tuple2<String, Integer>> resultList = result.collect();
        System.out.println("Actual: " + resultList);

        List<Tuple2<String, Integer>> expected = Arrays.asList(
                Tuple2.of("A", 1),
                Tuple2.of("D", 2)
        );

        Assertions.assertEquals(expected, resultList);
    }


}
