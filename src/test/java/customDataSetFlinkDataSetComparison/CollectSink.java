package customDataSetFlinkDataSetComparison;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

public class CollectSink<T> extends RichSinkFunction<T> {
    public static final List<Object> values = new ArrayList<>();

    @Override
    public void invoke(T value, Context context) {
        synchronized (values) {
            values.add(value);
        }
    }

    @Override
    public void open(Configuration parameters) {
        values.clear();
    }

    public static <T> List<T> getValues() {
        synchronized (values) {
            List<T> result = new ArrayList<>();
            for (Object value : values) {
                result.add((T) value);
            }
            return result;
        }
    }
}

