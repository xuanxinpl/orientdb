/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.graph.importer;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public abstract class OAbstractBaseOperation implements Operation {
  protected OrientVertex lookupVertex(final OGraphImporter importer, final OrientBaseGraph graph, final String vertexClassName,
      final Object id) {
    final OIdentifiable record = (OIdentifiable) importer.getVertexIndex(graph, vertexClassName).get(id);
    if (record == null)
      return null;
    if (record instanceof OrientVertex)
      return (OrientVertex) record;
    return new OrientVertex(graph, record);
  }

  public static String getThreadClusterName(final OrientBaseGraph graph, final String className, final int threadId) {
    final int[] clusterIds = graph.getRawGraph().getMetadata().getSchema().getClass(className).getClusterIds();
    return graph.getRawGraph().getClusterNameById(clusterIds[threadId]);
  }

}