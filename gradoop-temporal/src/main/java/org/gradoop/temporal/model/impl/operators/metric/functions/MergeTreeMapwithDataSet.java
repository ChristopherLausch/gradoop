package org.gradoop.temporal.model.impl.operators.metric.functions;

import com.esotericsoftware.kryo.util.IntMap;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;
import java.util.TreeMap;

public class MergeTreeMapwithDataSet implements CrossFunction<Tuple2<GradoopId, TreeMap<Long, Integer>>, Tuple1<Long>, Tuple2<Long, Integer>> {
    @Override
    public Tuple2<Long, Integer> cross(
            Tuple2<GradoopId, TreeMap<Long, Integer>> input1,
            Tuple1<Long> input2) throws Exception {

        GradoopId id = input1.f0;
        TreeMap<Long, Integer> treeMap = input1.f1;
        Long timestampFromDataset = input2.f0;

        // If the TreeMap contains the timestamp, use its value
        if (treeMap.containsKey(timestampFromDataset)) {
            return new Tuple2<>(timestampFromDataset, treeMap.get(timestampFromDataset));

            //return new Tuple2<>(id, treeMap);
        }

        // Find the next lower timestamp in the TreeMap
        Long lowerTimestamp = treeMap.floorKey(timestampFromDataset);

        // If there is no lower timestamp, use the first entry in the TreeMap
        Integer value = 0;
        if (lowerTimestamp != null) {
            value = treeMap.get(lowerTimestamp);
        }

        // Create a new TreeMap with the updated value
        //TreeMap<Long, Integer> newTreeMap = new TreeMap<>(treeMap);
        return new Tuple2<>(timestampFromDataset, value);

        //return new Tuple2<>(id, newTreeMap);

        // ziel return return tuple2<long, integer>
    }
}
