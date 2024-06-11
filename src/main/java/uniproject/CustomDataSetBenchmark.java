package uniproject;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Diese Klasse enthält Benchmarks für ein benutzerdefiniertes DataSet-System.
 * Sie verwendet JMH (Java Microbenchmark Harness) für die Performance-Analyse.
 */
@State(Scope.Benchmark)
public class CustomDataSetBenchmark {

    private List<Tuple3<Integer, Integer, Long>> dataset;
    private CustomDataSetImpl<Tuple3<Integer, Integer, Long>> customDataSet;

    /**
     * Setzt die Benchmark-Umgebung auf. Diese Methode wird einmal pro Durchlauf ausgeführt.
     * Hier wird ein großes DataSet von Tupeln generiert, das für die Benchmarks verwendet wird.
     */
    @Setup(Level.Trial)
    public void setUp() {
        dataset = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            dataset.add(new Tuple3<>(i, i * 2, System.currentTimeMillis()));
        }
        customDataSet = new CustomDataSetImpl<>(dataset);
    }

    /**
     * Führt einen Benchmark für das Filtern von DataSet-Elementen durch.
     * Misst die durchschnittliche Zeit, die benötigt wird, um Elemente basierend auf einer Bedingung zu filtern.
     * @param blackhole ein Blackhole-Objekt, das verhindert, dass JVM-Optimierungen das Benchmark-Ergebnis verzerren.
     * @throws Exception falls während des Benchmarks eine Ausnahme auftritt.
     */
    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void testCustomDataSetFilter(Blackhole blackhole) throws Exception {
        CustomDataSet<Tuple3<Integer, Integer, Long>> result = customDataSet.filter(new FilterFunction<Tuple3<Integer, Integer, Long>>() {
            @Override
            public boolean filter(Tuple3<Integer, Integer, Long> value) {
                return value.f0 % 2 == 0;
            }
        });
        blackhole.consume(result);
    }

}
