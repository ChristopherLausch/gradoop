/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.temporal.model.impl.operators.metric.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.TreeSet;
import java.util.TreeMap;
import java.util.SortedSet;
import java.util.HashMap;
import java.util.Map;


public class GroupDegreeTreesToAggregateDegrees
        implements GroupReduceFunction<Tuple2<Long, Integer>, Tuple2<Long, Float>> {

    /**
     * The aggregate type to use (min,max,avg).
     */
    private final AggregationType aggregateType;

    /**
     * Creates an instance of this group reduce function.
     *
     * @param aggregateType the aggregate type to use (min,max,avg).
     */
    public GroupDegreeTreesToAggregateDegrees(AggregationType aggregateType) {
        this.aggregateType = aggregateType;
    }
    @Override
    public void reduce(Iterable<Tuple2<Long, Integer>> values, Collector<Tuple2<Long, Float>> out) {
        long timestamp = 0;
        float aggregationResult = 0;
        switch (aggregateType) {
            case MIN:
                for (Tuple2<Long, Integer> value : values) {
                    timestamp = value.f0;
                    aggregationResult = Math.min(aggregationResult, value.f1);
                }
                break;
            case MAX:
                // Choose your aggregation logic (e.g., max, min, avg)
                for (Tuple2<Long, Integer> value : values) {
                    timestamp = value.f0;
                    aggregationResult = Math.max(aggregationResult, value.f1);
                }
                break;

            case AVG:
                int sum = 0;
                int count = 0;
                // Calculate the sum and count for each timestamp
                for (Tuple2<Long, Integer> value : values) {
                    timestamp = value.f0;
                    sum += value.f1;
                    count++;
                }
                aggregationResult = (float) sum / count;
                break;

            default:
                throw new IllegalArgumentException("Aggregate type not specified.");

        }

        out.collect(new Tuple2<>(timestamp, aggregationResult));
    }
}
