package customDataSetFlinkDataSetComparison;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import uniproject.CustomDataSet;
import uniproject.CustomDataSetImpl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Testklasse, die Funktionalitäten von Apache Flink und einem benutzerdefinierten DataSet-System vergleicht.
 * Die Tests umfassen verschiedene Datenverarbeitungsfunktionen wie Map, Distinct, Filter, und Union.
 */
public class DataSetComparisonTest {

    /**
     * Testet die Map-Funktion, die jedes Element des DataSets transformiert.
     * Dieser Test validiert die Konsistenz zwischen den Ergebnissen von Flink und Custom DataSet.
     */
    @Test
    public void testMapFunction() throws Exception {
        List<Tuple3<Integer, Integer, Long>> input = readTestData("src/test/java/customDataSetFlinkDataSetComparison/testdata.txt");

        ExecutionEnvironment flinkEnv = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, Integer, Long>> flinkDataSet = flinkEnv.fromCollection(input);

        DataSet<Tuple3<Integer, Integer, Long>> flinkResult = flinkDataSet.map(new MapFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>() {
            @Override
            public Tuple3<Integer, Integer, Long> map(Tuple3<Integer, Integer, Long> value) {
                return Tuple3.of(value.f0 * 2, value.f1 * 2, value.f2);
            }
        });

        List<Tuple3<Integer, Integer, Long>> flinkResultList = flinkResult.collect();
        flinkResultList.sort(Comparator.comparingInt(Tuple3::hashCode));

        CustomDataSet<Tuple3<Integer, Integer, Long>> customDataSet = new CustomDataSetImpl<>(input, TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>(){}));
        CustomDataSet<Tuple3<Integer, Integer, Long>> customResult = customDataSet.map(new MapFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>() {
            @Override
            public Tuple3<Integer, Integer, Long> map(Tuple3<Integer, Integer, Long> value) {
                return Tuple3.of(value.f0 * 2, value.f1 * 2, value.f2);
            }
        });

        List<Tuple3<Integer, Integer, Long>> customResultList = customResult.collect();
        customResultList.sort(Comparator.comparingInt(Tuple3::hashCode));

        Assertions.assertEquals(flinkResultList, customResultList);
    }

    /**
     * Testet die Distinct-Funktion, die Duplikate aus einem DataSet entfernt.
     * Überprüft, ob die Ergebnisse von Flink und Custom DataSet identisch sind.
     */
    @Test
    public void testDistinctFunction() throws Exception {
        List<Tuple3<Integer, Integer, Long>> input = readTestData("src/test/java/customDataSetFlinkDataSetComparison/testdata.txt");

        ExecutionEnvironment flinkEnv = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, Integer, Long>> flinkDataSet = flinkEnv.fromCollection(input);

        DataSet<Tuple3<Integer, Integer, Long>> flinkResult = flinkDataSet.distinct();

        List<Tuple3<Integer, Integer, Long>> flinkResultList = flinkResult.collect();
        flinkResultList.sort(Comparator.comparingInt(Tuple3::hashCode));

        CustomDataSet<Tuple3<Integer, Integer, Long>> customDataSet = new CustomDataSetImpl<>(input, TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>(){}));
        CustomDataSet<Tuple3<Integer, Integer, Long>> customResult = customDataSet.distinct();

        List<Tuple3<Integer, Integer, Long>> customResultList = customResult.collect();
        customResultList.sort(Comparator.comparingInt(Tuple3::hashCode));

        Assertions.assertEquals(flinkResultList, customResultList);
    }

    /**
     * Testet die Filter-Funktion, die Elemente basierend auf einer Bedingung auswählt.
     * Vergleicht das Verhalten und die Konsistenz der Ergebnisse zwischen Flink und Custom DataSet.
     */
    @Test
    public void testFilterFunction() throws Exception {
        List<Tuple3<Integer, Integer, Long>> input = readTestData("src/test/java/customDataSetFlinkDataSetComparison/testdata.txt");

        ExecutionEnvironment flinkEnv = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, Integer, Long>> flinkDataSet = flinkEnv.fromCollection(input);

        DataSet<Tuple3<Integer, Integer, Long>> flinkResult = flinkDataSet.filter(new FilterFunction<Tuple3<Integer, Integer, Long>>() {
            @Override
            public boolean filter(Tuple3<Integer, Integer, Long> value) {
                return value.f0 % 2 == 0;
            }
        });

        List<Tuple3<Integer, Integer, Long>> flinkResultList = flinkResult.collect();
        flinkResultList.sort(Comparator.comparingInt(Tuple3::hashCode));
        CustomDataSet<Tuple3<Integer, Integer, Long>> customDataSet = new CustomDataSetImpl<>(input, TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>(){}));
        CustomDataSet<Tuple3<Integer, Integer, Long>> customResult = customDataSet.filter(new FilterFunction<Tuple3<Integer, Integer, Long>>() {
            @Override
            public boolean filter(Tuple3<Integer, Integer, Long> value) {
                return value.f0 % 2 == 0;
            }
        });

        List<Tuple3<Integer, Integer, Long>> customResultList = customResult.collect();
        customResultList.sort(Comparator.comparingInt(Tuple3::hashCode));

        Assertions.assertEquals(flinkResultList, customResultList);
    }

    /**
     * Testet die Union-Funktion, die zwei DataSets kombiniert.
     * Überprüft, ob die zusammengesetzten Ergebnisse von Flink und Custom DataSet gleich sind.
     */
    @Test
    public void testUnionFunction() throws Exception {
        // Erstellen der ersten Eingabeliste
        List<Tuple3<Integer, Integer, Long>> input1 = readTestData("src/test/java/customDataSetFlinkDataSetComparison/testdata.txt");

        // Erstellen der zweiten Eingabeliste
        List<Tuple3<Integer, Integer, Long>> input2 = readTestData("src/test/java/customDataSetFlinkDataSetComparison/testdata2.txt");

        // Flink Umgebung und DataSets erstellen
        ExecutionEnvironment flinkEnv = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, Integer, Long>> flinkDataSet1 = flinkEnv.fromCollection(input1);
        DataSet<Tuple3<Integer, Integer, Long>> flinkDataSet2 = flinkEnv.fromCollection(input2);

        // Union der beiden Flink DataSets
        DataSet<Tuple3<Integer, Integer, Long>> flinkResult = flinkDataSet1.union(flinkDataSet2);

        // Sammeln und Sortieren der Flink Ergebnisse
        List<Tuple3<Integer, Integer, Long>> flinkResultList = flinkResult.collect();
        flinkResultList.sort(new Tuple3Comparator());

        // CustomDataSets erstellen
        CustomDataSet<Tuple3<Integer, Integer, Long>> customDataSet1 = new CustomDataSetImpl<>(input1, TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>(){}));
        CustomDataSet<Tuple3<Integer, Integer, Long>> customDataSet2 = new CustomDataSetImpl<>(input2, TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>(){}));

        // Union der beiden CustomDataSets
        CustomDataSet<Tuple3<Integer, Integer, Long>> customResult = customDataSet1.union(customDataSet2);

        // Sammeln und Sortieren der CustomDataSet Ergebnisse
        List<Tuple3<Integer, Integer, Long>> customResultList = customResult.collect();
        customResultList.sort(new Tuple3Comparator());

        // Vergleichen der Ergebnisse
        Assertions.assertEquals(flinkResultList, customResultList);
    }

    /**
     * Hilfsfunktion zum Lesen von Testdaten aus einer Datei.
     * @param fileName Der Dateiname der Testdaten.
     * @return Eine Liste von Tuple3, die aus der Datei gelesen wurden.
     * @throws Exception bei Fehlern beim Lesen der Datei.
     */
    private List<Tuple3<Integer, Integer, Long>> readTestData(String fileName) throws Exception {
        List<Tuple3<Integer, Integer, Long>> data = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new java.io.FileInputStream(fileName), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                int f0 = Integer.parseInt(parts[0]);
                int f1 = Integer.parseInt(parts[1]);
                long f2 = Long.parseLong(parts[2]);
                data.add(Tuple3.of(f0, f1, f2));
            }
        }
        return data;
    }

    /**
     * Testet die FlatMap-Funktion, die jedes Eingabeelement in mehrere Ausgabeelemente transformieren kann.
     * Dieser Test vergleicht das Verhalten der FlatMap-Operationen zwischen Flink und Custom DataSet.
     */
    @Test
    public void testFlatMapFunction() throws Exception {
        List<Tuple3<Integer, Integer, Long>> input = readTestData("src/test/java/customDataSetFlinkDataSetComparison/testdata.txt");

        ExecutionEnvironment flinkEnv = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, Integer, Long>> flinkDataSet = flinkEnv.fromCollection(input);

        DataSet<Tuple2<Integer, Long>> flinkResult = flinkDataSet.flatMap(new FlatMapFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Long>>() {
            @Override
            public void flatMap(Tuple3<Integer, Integer, Long> value, Collector<Tuple2<Integer, Long>> out) {
                out.collect(Tuple2.of(value.f0, value.f2));
                out.collect(Tuple2.of(value.f1, value.f2));
            }
        });

        List<Tuple2<Integer, Long>> flinkResultList = flinkResult.collect();
        flinkResultList.sort(Comparator.comparing(Tuple2::toString));

        CustomDataSet<Tuple3<Integer, Integer, Long>> customDataSet = new CustomDataSetImpl<>(input, TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>(){}));
        CustomDataSet<Tuple2<Integer, Long>> customResult = customDataSet.flatMap(new FlatMapFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Long>>() {
            @Override
            public void flatMap(Tuple3<Integer, Integer, Long> value, Collector<Tuple2<Integer, Long>> out) {
                out.collect(Tuple2.of(value.f0, value.f2));
                out.collect(Tuple2.of(value.f1, value.f2));
            }
        });

        List<Tuple2<Integer, Long>> customResultList = customResult.collect();
        customResultList.sort(Comparator.comparing(Tuple2::toString));

        Assertions.assertEquals(flinkResultList, customResultList);
    }

   // @Test
  //  public void testGroupByFunction() throws Exception {
  //      List<Tuple3<Integer, Integer, Long>> input = readTestData("src/test/java/customDataSetFlinkDataSetComparison/testdata.txt");
//
  //      // Flink DataSet API
  //      ExecutionEnvironment flinkEnv = ExecutionEnvironment.getExecutionEnvironment();
  //      DataSet<Tuple3<Integer, Integer, Long>> flinkDataSet = flinkEnv.fromCollection(input);
//
  //      DataSet<Tuple2<Integer, Long>> flinkResult = flinkDataSet
  //              .groupBy(0)
  //              .reduceGroup(new SumGroupReduceFunction());
//
  //      List<Tuple2<Integer, Long>> flinkResultList = flinkResult.collect();
  //      flinkResultList.sort(Comparator.comparing(Tuple2::toString));
//
  //      // CustomDataSet API
  //      CustomDataSet<Tuple3<Integer, Integer, Long>> customDataSet = new CustomDataSetImpl<>(input, TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>(){}));
  //      CustomDataSet<Tuple2<Integer, Long>> customResult = customDataSet
  //              .groupBy(0)
  //              .reduceGroup(new SumGroupReduceFunction());
//
  //      List<Tuple2<Integer, Long>> customResultList = customResult.collect();
  //      customResultList.sort(Comparator.comparing(Tuple2::toString));
//
  //      // Vergleich der Ergebnisse
  //      Assertions.assertEquals(flinkResultList, customResultList);
  //  }


    /**
     * Testet die Join-Funktion, die zwei DataSets anhand eines Schlüssels verbindet.
     * Überprüft die Gleichheit der Verknüpfungsergebnisse von Flink und Custom DataSet.
     */
    @Test
    public void testJoinFunction() throws Exception {
        // Read the test data for the first and second datasets
        List<Tuple3<Integer, Integer, Long>> input1 = readTestData("src/test/java/customDataSetFlinkDataSetComparison/testdata.txt");
        List<Tuple3<Integer, Integer, Long>> input2 = readTestData("src/test/java/customDataSetFlinkDataSetComparison/testdata2.txt");

        // Flink DataSet API
        ExecutionEnvironment flinkEnv = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, Integer, Long>> flinkDataSet1 = flinkEnv.fromCollection(input1);
        DataSet<Tuple3<Integer, Integer, Long>> flinkDataSet2 = flinkEnv.fromCollection(input2);

        DataSet<Tuple2<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>> flinkResult = flinkDataSet1
                .join(flinkDataSet2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple2<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>>() {
                    @Override
                    public Tuple2<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>> join(Tuple3<Integer, Integer, Long> first, Tuple3<Integer, Integer, Long> second) {
                        return Tuple2.of(first, second);
                    }
                });

        List<Tuple2<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>> flinkResultList = flinkResult.collect();
        flinkResultList.sort(Comparator.comparing(Tuple2::toString)); // Sortieren nach toString

        // CustomDataSet API
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        CustomDataSet<Tuple3<Integer, Integer, Long>> customDataSet1 = new CustomDataSetImpl<>(streamEnv, input1, TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>(){}));
        CustomDataSet<Tuple3<Integer, Integer, Long>> customDataSet2 = new CustomDataSetImpl<>(streamEnv, input2, TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>(){}));

        ConnectedStreams<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>> connectedStreams = customDataSet1.join(customDataSet2);
        DataStream<Tuple2<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>> customResult = connectedStreams
                .keyBy(new KeySelector<Tuple3<Integer, Integer, Long>, Integer>() {
                    @Override
                    public Integer getKey(Tuple3<Integer, Integer, Long> value) {
                        return value.f0;
                    }
                }, new KeySelector<Tuple3<Integer, Integer, Long>, Integer>() {
                    @Override
                    public Integer getKey(Tuple3<Integer, Integer, Long> value) {
                        return value.f0;
                    }
                })
                .process(new CustomDataSetImpl.CustomJoinCoProcessFunction<>());

        // Use custom sink to collect results
        customResult.addSink(new CollectSink<>());

        // Execute the Flink job
        streamEnv.execute("CustomDataSet Execution");

        // Retrieve the results from the custom sink
        List<Tuple2<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>> customResultList = CollectSink.getValues();
        customResultList.sort(Comparator.comparing(Tuple2::toString)); // Sortieren nach toString

        // Vergleich der Ergebnisse
        Assertions.assertEquals(flinkResultList, customResultList);
    }



//    @Test
//    public void testDifferenceFunction() throws Exception {
//        // Example data
//        List<Tuple2<String, Long>> data1 = Arrays.asList(
//                new Tuple2<>("A", 1L),
//                new Tuple2<>("B", 1L),
//                new Tuple2<>("C", 1L)
//        );
//
//        List<Tuple2<String, Long>> data2 = Arrays.asList(
//                new Tuple2<>("B", 2L),
//                new Tuple2<>("C", 2L),
//                new Tuple2<>("D", 2L)
//        );
//
//        // Flink DataSet API
//        ExecutionEnvironment flinkEnv = ExecutionEnvironment.getExecutionEnvironment();
//        DataSet<Tuple2<String, Long>> flinkDataSet1 = flinkEnv.fromCollection(data1);
//        DataSet<Tuple2<String, Long>> flinkDataSet2 = flinkEnv.fromCollection(data2);
//
//        // Apply the difference operation using Flink DataSet API
//        DataSet<Tuple2<String, Long>> flinkUnionResult = flinkDataSet1.union(flinkDataSet2);
//        DataSet<Tuple2<String, Long>> flinkDifferenceResult = flinkUnionResult
//                .groupBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
//                .reduceGroup(new RemoveDuplicates<>());
//
//        List<Tuple2<String, Long>> flinkResultList = flinkDifferenceResult.collect();
//        flinkResultList.sort(Comparator.comparing(Tuple2::toString)); // Sort by toString
//
//        // CustomDataSet API
//        CustomDataSet<Tuple2<String, Long>> customDataSet1 = new CustomDataSetImpl<>(data1);
//        CustomDataSet<Tuple2<String, Long>> customDataSet2 = new CustomDataSetImpl<>(data2);
//
//        // Apply the difference operation using CustomDataSet API
//        CustomDataSet<Tuple2<String, Long>> customUnionResult = customDataSet1.union(customDataSet2);
//        CustomDataSet<Tuple2<String, Long>> customDifferenceResult = customUnionResult
//                .groupBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
//                .reduceGroup(new RemoveDuplicates<>());
//
//        List<Tuple2<String, Long>> customResultList = customDifferenceResult.collect();
//        customResultList.sort(Comparator.comparing(Tuple2::toString)); // Sort by toString
//
//        // Compare the results
//        Assertions.assertEquals(flinkResultList, customResultList);
//    }

    private static class RemoveDuplicates<T extends Tuple2<String, Long>> implements GroupReduceFunction<T, T> {
        @Override
        public void reduce(Iterable<T> values, Collector<T> out) {
            List<T> list = new ArrayList<>();
            for (T value : values) {
                list.add(value);
            }
            if (list.size() == 1) {
                out.collect(list.get(0));
            }
        }
    }

    /**
     * Testet die Differenz-Funktion, die Elemente identifiziert, die in einem DataSet vorhanden sind und im anderen fehlen.
     * Der Test wird verwendet, um die Implementierung der Differenzfunktion in beiden Systemen zu verifizieren.
     */
    @Test
    public void testDifferenceOperator() throws Exception {
        List<Tuple2<String, Long>> data1 = Arrays.asList(
                new Tuple2<>("A", 1L),
                new Tuple2<>("B", 2L),
                new Tuple2<>("C", 3L)
        );

        List<Tuple2<String, Long>> data2 = Arrays.asList(
                new Tuple2<>("B", 2L),
                new Tuple2<>("C", 3L),
                new Tuple2<>("D", 4L)
        );

        CustomDataSet<Tuple2<String, Long>> customDataSet1 = new CustomDataSetImpl<>(data1, TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){}));
        CustomDataSet<Tuple2<String, Long>> customDataSet2 = new CustomDataSetImpl<>(data2, TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){}));

        CustomDataSet<Tuple2<String, Long>> unionDataSet = customDataSet1.union(customDataSet2);

        List<Tuple2<String, Long>> unionResult = unionDataSet.collect();

        List<Tuple2<String, Long>> expectedResult = Arrays.asList(
                new Tuple2<>("A", 1L),
                new Tuple2<>("B", 2L),
                new Tuple2<>("C", 3L),
                new Tuple2<>("B", 2L),
                new Tuple2<>("C", 3L),
                new Tuple2<>("D", 4L)
        );

        Assertions.assertEquals(expectedResult.size(), unionResult.size());
        Assertions.assertTrue(unionResult.containsAll(expectedResult));
    }

    private static class SumGroupReduceFunction implements GroupReduceFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Long>> {
        @Override
        public void reduce(Iterable<Tuple3<Integer, Integer, Long>> values, Collector<Tuple2<Integer, Long>> out) {
            long sum = 0;
            int key = 0;
            for (Tuple3<Integer, Integer, Long> value : values) {
                key = value.f0;
                sum += value.f2;
            }
            out.collect(Tuple2.of(key, sum));
        }
    }
    /**
     * Inner class zum Sortieren von Tuple3-Elementen basierend auf mehreren Kriterien.
     */
    private static class Tuple3Comparator implements Comparator<Tuple3<Integer, Integer, Long>> {
        @Override
        public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
            int comp0 = Integer.compare(o1.f0, o2.f0);
            if (comp0 != 0) {
                return comp0;
            }
            int comp1 = Integer.compare(o1.f1, o2.f1);
            if (comp1 != 0) {
                return comp1;
            }
            return Long.compare(o1.f2, o2.f2);
        }
    }
}
