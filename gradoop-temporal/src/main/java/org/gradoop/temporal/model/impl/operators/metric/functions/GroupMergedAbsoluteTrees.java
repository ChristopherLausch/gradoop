package org.gradoop.temporal.model.impl.operators.metric.functions;


import org.antlr.v4.runtime.tree.Tree;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class GroupMergedAbsoluteTrees implements GroupReduceFunction<Tuple2<GradoopId, TreeMap<Long, Integer>>,
        Tuple2<GradoopId, TreeMap<Long, Integer>>> {

    @Override
    public void reduce(Iterable<Tuple2<GradoopId, TreeMap<Long, Integer>>> values,
                       Collector<Tuple2<GradoopId, TreeMap<Long, Integer>>> out) {

        // Create a TreeMap to store merged values
        TreeMap<Long, Integer> mergedMap = new TreeMap<>();

        GradoopId id = GradoopId.NULL_VALUE;

        // Iterate through the values and merge TreeMap values
        for (Tuple2<GradoopId, TreeMap<Long, Integer>> tuple : values) {
            TreeMap<Long, Integer> currentMap = tuple.f1;

            if(!(id ==tuple.f0)){
                id = tuple.f0;
            }

            for (Map.Entry<Long, Integer> entry : currentMap.entrySet()) {
                Long key = entry.getKey();
                Integer value = entry.getValue();

                // Merge values for the same key
                mergedMap.merge(key, value, Integer::min);
            }
        }

        // Emit the result
        out.collect(new Tuple2<>(id, mergedMap));
    }
}
