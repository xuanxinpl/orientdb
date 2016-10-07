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

import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.Map;

public class OCreateEdgeOperation extends OAbstractBaseOperation {
  private final String              edgeClassName;
  private final String              sourceVertexClassName;
  private final Object              sourceVertexId;
  private final String              destinationVertexClassName;
  private final Object              destinationVertexId;
  private final Map<String, Object> properties;

  public OCreateEdgeOperation(final String edgeClassName, final String sourceVertexClassName, final Object sourceVertexId,
      final String destinationVertexClassName, final Object destinationVertexId, final Map<String, Object> properties) {
    this.edgeClassName = edgeClassName;
    this.sourceVertexClassName = sourceVertexClassName;
    this.sourceVertexId = sourceVertexId;
    this.destinationVertexClassName = destinationVertexClassName;
    this.destinationVertexId = destinationVertexId;
    this.properties = properties;
  }

  public void execute(final OGraphImporter importer, final OrientBaseGraph graph, final int sourceClusterIndex,
      final int destinationClusterIndex) {

    OrientVertex sourceVertex = lookupVertex(importer, graph, sourceVertexClassName, sourceVertexId);
    if (sourceVertex == null) {
      // CREATE SOURCE VERTEX
      importer.lockVertexCreationByKey(sourceVertexClassName, sourceVertexId);

      sourceVertex = graph.addTemporaryVertex(sourceVertexClassName, properties);
      sourceVertex.getRecord().field(importer.getRegisteredVertexClass(sourceVertexClassName).getKey(), sourceVertexId);
      sourceVertex.save(getThreadClusterName(graph, sourceVertexClassName, sourceClusterIndex));
    }

    OrientVertex destinationVertex = lookupVertex(importer, graph, destinationVertexClassName, destinationVertexId);
    if (destinationVertex == null) {
      // CREATE DESTINATION VERTEX
      importer.lockVertexCreationByKey(destinationVertexClassName, destinationVertexId);

      destinationVertex = graph.addTemporaryVertex(destinationVertexClassName, properties);
      destinationVertex.getRecord().field(importer.getRegisteredVertexClass(destinationVertexClassName).getKey(),
          destinationVertexId);
      destinationVertex.save(getThreadClusterName(graph, destinationVertexClassName, destinationClusterIndex));
    }

    sourceVertex.addEdge(null, destinationVertex, edgeClassName, getThreadClusterName(graph, edgeClassName, sourceClusterIndex),
        properties);
  }
}
