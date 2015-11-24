package org.gradoop.model.impl.operators.equality.collection;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.counting.OneInTuple1;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.equality.EqualityBase;

/**
 * Created by peet on 17.11.15.
 */
public class EqualByGraphIds
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityBase
  implements BinaryCollectionToValueOperator<G, V, E, Boolean> {

  @Override
  public DataSet<Boolean> execute(
    GraphCollection<V, E, G> firstCollection,
    GraphCollection<V, E, G> secondCollection) {

    DataSet<Tuple2<GradoopId, Long>> firstGraphIdsWithCount =
      getIdsWithCount(firstCollection);

    DataSet<Tuple2<GradoopId, Long>> secondGraphIdsWithCount =
      getIdsWithCount(secondCollection);

    DataSet<Tuple1<Long>> distinctFirstIdCount = firstGraphIdsWithCount
      .map(new OneInTuple1<Tuple2<GradoopId, Long>>())
      .sum(0);

    DataSet<Tuple1<Long>> matchingIdCount = firstGraphIdsWithCount
      .join(secondGraphIdsWithCount)
      .where(0, 1).equalTo(0, 1)
      .with(new OneInTuple1<Tuple2<GradoopId,Long>>())
      .sum(0);

    return checkCountEqualsCount(distinctFirstIdCount, matchingIdCount);
  }



  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}