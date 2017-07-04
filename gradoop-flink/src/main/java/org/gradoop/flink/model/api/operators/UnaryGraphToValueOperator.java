/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.LogicalGraph;

/**
 * Creates a value from one input {@link org.gradoop.flink.model.impl.LogicalGraph}.
 *
 * @param <T> result type
 */
public interface UnaryGraphToValueOperator<T> {
  /**
   * Executes the operator.
   *
   * @param graph input graph
   * @return operator result
   */
  T execute(LogicalGraph graph);
}