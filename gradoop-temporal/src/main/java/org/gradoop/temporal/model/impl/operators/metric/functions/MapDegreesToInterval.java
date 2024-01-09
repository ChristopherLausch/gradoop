package org.gradoop.temporal.model.impl.operators.metric.functions;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class MapDegreesToInterval implements MapPartitionFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>> {
    @Override
    public void mapPartition(Iterable<Tuple2<Long, Float>> values, Collector<Tuple3<Long, Long, Float>> out) {
        Long startTimestamp = null;
        Long endTimestamp = null;
        Float value = null;

        for (Tuple2<Long, Float> tuple : values) {
            if (startTimestamp == null) {
                out.collect(new Tuple3<>(Long.MIN_VALUE, tuple.f0, 0f));
                // First element in the group
                startTimestamp = tuple.f0;
                endTimestamp = tuple.f0;
                value = tuple.f1;
            } else {
                if (!tuple.f1.equals(value)) {
                    // Value changed, emit the current interval and start a new one
                    out.collect(new Tuple3<>(startTimestamp, tuple.f0, value));
                    startTimestamp = tuple.f0;
                    endTimestamp = tuple.f0;
                    value = tuple.f1;
                } else {
                    // Extend the current interval
                    endTimestamp = tuple.f0;
                }
            }
        }

        // Emit the last interval in the group
        if (startTimestamp != null) {
           // out.collect(new Tuple3<>(startTimestamp, endTimestamp + 1, value));
        }
    }

}