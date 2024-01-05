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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.metric.functions.*;
import org.gradoop.temporal.util.TemporalGradoopConfig;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.Int;

import java.util.*;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class DegreeEvolutionTest extends TemporalGradoopTestBase {
    /**
     * The expected in-degrees for each vertex label.
     */
    private static final List<Tuple3<Long, Long, Float>> EXPECTED_IN_DEGREES = new ArrayList<>();
    /**
     * The expected out-degrees for each vertex label.
     */
    private static final List<Tuple3<Long, Long, Float>> EXPECTED_OUT_DEGREES = new ArrayList<>();
    /**
     * The expected degrees for each vertex label.
     */
    private static final List<Tuple3<Long, Long, Float>> EXPECTED_BOTH_DEGREES = new ArrayList<>();

    static {
        // IN DEGREES
        EXPECTED_IN_DEGREES.add(new Tuple3<>(Long.MIN_VALUE, 0L, 0.0f));
        EXPECTED_IN_DEGREES.add(new Tuple3<>(0L, 4L, 0.25f));
        EXPECTED_IN_DEGREES.add(new Tuple3<>(4L, 5L, 1.0f));
        EXPECTED_IN_DEGREES.add(new Tuple3<>(5L, 6L, 0.5f));
        EXPECTED_IN_DEGREES.add(new Tuple3<>(6L, 7L, 0.5f));
        EXPECTED_IN_DEGREES.add(new Tuple3<>(7L, Long.MAX_VALUE, 0.25f));

        // OUT DEGREES
        EXPECTED_OUT_DEGREES.add(new Tuple3<>(Long.MIN_VALUE, 0L, 0.0f));
        EXPECTED_OUT_DEGREES.add(new Tuple3<>(0L, 4L, 0.25f));
        EXPECTED_OUT_DEGREES.add(new Tuple3<>(4L, 5L, 1.0f));
        EXPECTED_OUT_DEGREES.add(new Tuple3<>(5L, 6L, 0.5f));
        EXPECTED_OUT_DEGREES.add(new Tuple3<>(6L, 7L, 0.5f));
        EXPECTED_OUT_DEGREES.add(new Tuple3<>(7L, Long.MAX_VALUE, 0.25f));

        // DEGREES
        EXPECTED_BOTH_DEGREES.add(new Tuple3<>(Long.MIN_VALUE, 0L, 0.0f));
        EXPECTED_BOTH_DEGREES.add(new Tuple3<>(0L, 4L, 0.4f));
        EXPECTED_BOTH_DEGREES.add(new Tuple3<>(4L, 5L, 1.6f));
        EXPECTED_BOTH_DEGREES.add(new Tuple3<>(5L, 6L, 0.8f));
        EXPECTED_BOTH_DEGREES.add(new Tuple3<>(6L, 7L, 0.8f));
        EXPECTED_BOTH_DEGREES.add(new Tuple3<>(7L, Long.MAX_VALUE, 0.4f));
    }

    /**
     * The degree type to test.
     */
    @Parameterized.Parameter(0)
    public VertexDegree degreeType;

    /**
     * The expected degree evolution for the given type.
     */
    @Parameterized.Parameter(1)
    public List<Tuple3<Long, Long, Float>> expectedDegrees;

    /**
     * The temporal graph to test the operator.
     */
    TemporalGraph testGraph;

    /**
     * The parameters to test the operator.
     *
     * @return three different vertex degree types with its corresponding expected degree evolution.
     */
    @Parameterized.Parameters(name = "Test degree type {0}.")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{VertexDegree.IN, EXPECTED_IN_DEGREES},
                new Object[]{VertexDegree.OUT, EXPECTED_OUT_DEGREES},
                new Object[]{VertexDegree.BOTH, EXPECTED_BOTH_DEGREES});
    }

    /**
     * Set up the test graph and create the id-label mapping.
     *
     * @throws Exception in case of an error
     */
    @Before
    public void setUp() throws Exception {
        testGraph = getTestGraphWithValues();
        Collection<Tuple2<GradoopId, String>> idLabelCollection = new HashSet<>();
        testGraph.getVertices().map(v -> new Tuple2<>(v.getId(), v.getLabel()))
                .returns(new TypeHint<Tuple2<GradoopId, String>>() {
                }).output(new LocalCollectionOutputFormat<>(idLabelCollection));
        getExecutionEnvironment().execute();
    }

    /**
     * Test the avg degree evolution operator.
     *
     * @throws Exception in case of an error.
     */
    @Test
    public void testMaxDegree() throws Exception {
        ExecutionEnvironment env = getExecutionEnvironment();
        /*DataSet<Tuple3<Long, Long, Float>> result = testGraph.callForValue(new MaxDegreeEvolution(degreeType, TimeDimension.VALID_TIME));
        result.print();
        */
        final DataSet<Tuple1<Long>> resultDataSet = testGraph.getEdges().flatMap(new GetTimestamps(TimeDimension.VALID_TIME, VertexDegree.BOTH)).distinct();
        final DataSet<Tuple1<Long>> sortedDataSet = resultDataSet.sortPartition(0, Order.ASCENDING).setParallelism(1);

        DataSet<Tuple2<GradoopId, TreeMap<Long, Integer>>> absoluteTree =  testGraph.getEdges()
                // 1) Extract vertex id(s) and corresponding time intervals
                .flatMap(new FlatMapVertexIdEdgeInterval(TimeDimension.VALID_TIME, degreeType))
                // 2) Group them by the vertex id
                .groupBy(0)
                // 3) For each vertex id, build a degree tree data structure
                .reduceGroup(new BuildTemporalDegreeTree())
                // 4) Transform each tree to aggregated evolution
                .map(new TransformDeltaToAbsoluteDegreeTree());

        absoluteTree.print();


        DataSet<Tuple2<GradoopId, TreeMap<Long, Integer>>> mergedTrees = absoluteTree
                .cross(sortedDataSet)
                .with(new MergeTreeMapwithDataSet())
                .groupBy(0)
                .reduceGroup(new GroupMergedAbsoluteTrees());

        mergedTrees.print();

        DataSet<Tuple2<Long, Integer>> trees = mergedTrees.flatMap(new FlatMapAbsoluteTreesToDataSet());

        DataSet<Tuple2<Long, Float>> max = trees.groupBy(0).reduceGroup(new GroupDegreeTreesToAggregateDegrees(AggregationType.MAX));

        DataSet<Tuple2<Long, Float>> max_sorted = max.sortPartition(0, Order.ASCENDING).setParallelism(1);

        max_sorted.print();

        DataSet<Tuple3<Long, Long, Float>> result = max_sorted.mapPartition(new MapDegreesToInterval());

        // Print the result or perform other actions
        result.print();
    }


}