package org.gradoop.examples.metric;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Identifiable;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.metric.functions.GetTimestamps;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;

public class DegreeEvolutionExample {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = getExecutionEnvironment();

        TemporalGraph graph = new TemporalCSVDataSource("2018-citibike-csv-1", TemporalGradoopConfig.createConfig(env))
                .getTemporalGraph();



        final DataSet<Tuple1<Long>> resultDataSet = graph.getEdges().flatMap(new GetTimestamps(TimeDimension.VALID_TIME, VertexDegree.BOTH)).distinct();

        resultDataSet.print();
    }
}