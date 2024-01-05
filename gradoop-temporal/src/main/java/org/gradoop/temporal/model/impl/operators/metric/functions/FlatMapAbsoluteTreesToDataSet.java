package org.gradoop.temporal.model.impl.operators.metric.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;
import java.util.TreeMap;

// FlatMap function to convert Tuple2<GradoopID, TreeMap<Long, Integer>> to Tuple2<Long, Integer>
public class FlatMapAbsoluteTreesToDataSet implements FlatMapFunction<Tuple2<GradoopId, TreeMap<Long, Integer>>, Tuple2<Long, Integer>> {
    @Override
    public void flatMap(Tuple2<GradoopId, TreeMap<Long, Integer>> input, Collector<Tuple2<Long, Integer>> out) {
        for (Map.Entry<Long, Integer> entry : input.f1.entrySet()) {
            out.collect(new Tuple2<>(entry.getKey(), entry.getValue()));
        }
    }
}