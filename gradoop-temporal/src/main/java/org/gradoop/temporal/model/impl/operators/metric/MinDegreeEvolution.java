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
package org.gradoop.temporal.model.impl.operators.metric;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.metric.functions.AggregationType;
import org.gradoop.temporal.model.impl.operators.metric.functions.GroupDegreeTreesToAggregateDegrees;
import org.gradoop.temporal.model.impl.operators.metric.functions.MapDegreesToInterval;

/**
 * Operator that calculates the evolution of the graph's minimum degree for the whole lifetime of the graph.
 * The result is a triple dataset {@link DataSet<Tuple3>} in form {@code <Long, Long, Float>}. It
 * represents a time interval (first and second element) and the aggregated degree value for this interval
 * (3rd element).
 */
public class MinDegreeEvolution extends BaseAggregateDegreeEvolution {

    /**
     * Creates an instance of this minimum degree evolution operator using {@link TimeDimension#VALID_TIME}
     * as default time dimension and {@link VertexDegree#BOTH} as default degree type.
     */
    public MinDegreeEvolution() {
        super();
    }

    /**
     * Creates an instance of this minimum degree evolution operator using the given time dimension and
     * degree type.
     *
     * @param degreeType the degree type (IN, OUT or BOTH)
     * @param dimension the time dimension to consider (VALID_TIME or TRANSACTION_TIME)
     */
    public MinDegreeEvolution(VertexDegree degreeType, TimeDimension dimension) {
        super(degreeType, dimension);
    }

    @Override
    public DataSet<Tuple3<Long, Long, Float>> execute(TemporalGraph graph) {
        return preProcess(graph)
                .reduceGroup(new GroupDegreeTreesToAggregateDegrees(AggregationType.MIN))
                .mapPartition(new MapDegreesToInterval());
    }
}