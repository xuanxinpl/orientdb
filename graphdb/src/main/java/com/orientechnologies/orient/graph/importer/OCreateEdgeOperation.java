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

import com.orientechnologies.common.log.OLogManager;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.Map;

/**
 * Creates an edge between two vertices.
 *
 * @author Luca Garulli (l.garulli-(at)-orientdb.com)
 */
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

  public void execute(final OGraphImporter importer, final OImporterWorkerThread workerThread, final OrientBaseGraph graph,
      final int sourceClusterIndex, final int destinationClusterIndex) {

    OrientVertex sourceVertex = null;
    OrientVertex destinationVertex = null;

    boolean needReload = false;

    // LOCK RESOURCES FIRST
    while (true) {
      if (importer.lockVertexCreationByKey(workerThread.getThreadId(), sourceVertexClassName, sourceVertexId)) {
        if (importer.lockVertexCreationByKey(workerThread.getThreadId(), destinationVertexClassName, destinationVertexId)) {
          // BOTH LOCKED
          break;
        } else {
          // UNLOCK SOURCE
          importer.unlockVertexCreationByKey(workerThread.getThreadId(), sourceVertexClassName, sourceVertexId);
        }
      }

      // CANNOT LOCK: COMMIT PENDING TX TO FREE ALL ACQUIRED LOCKS
      workerThread.commit(graph);
      needReload = true;

      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        return;
      }
    }

    sourceVertex = lookupVertex(importer, graph, sourceVertexClassName, sourceVertexId);
    if (sourceVertex == null) {
      // CREATE SOURCE VERTEX
      sourceVertex = graph.addTemporaryVertex(sourceVertexClassName);
      sourceVertex.getRecord().field(importer.getRegisteredVertexClass(sourceVertexClassName).getKey(), sourceVertexId);
      sourceVertex.save(getThreadClusterName(graph, sourceVertexClassName, sourceClusterIndex));
      if (importer.getVerboseLevel() > 2)
        OLogManager.instance().info(this, "%s created source vertex for key %s", workerThread.getThreadId(), sourceVertexId);
    } else {
      if (importer.getVerboseLevel() > 2)
        OLogManager.instance().info(this, "%s found record %s v%d for source key %s", workerThread.getThreadId(),
            sourceVertex.getIdentity(), sourceVertex.getRecord().getVersion(), sourceVertexId);

      if (needReload) {
        sourceVertex.getRecord().reload(null, false);

        if (importer.getVerboseLevel() > 2)
          OLogManager.instance().info(this, "%s reloaded record %s v%d for source key %s", workerThread.getThreadId(),
              sourceVertex.getIdentity(), sourceVertex.getRecord().getVersion(), sourceVertexId);
      }
    }

    destinationVertex = lookupVertex(importer, graph, destinationVertexClassName, destinationVertexId);
    if (destinationVertex == null) {
      // CREATE DESTINATION VERTEX
      destinationVertex = graph.addTemporaryVertex(destinationVertexClassName);
      destinationVertex.getRecord().field(importer.getRegisteredVertexClass(destinationVertexClassName).getKey(),
          destinationVertexId);
      destinationVertex.save(getThreadClusterName(graph, destinationVertexClassName, destinationClusterIndex));
      if (importer.getVerboseLevel() > 2)
        OLogManager.instance().info(this, "%s created destination vertex for key %s", workerThread.getThreadId(),
            destinationVertexId);
    } else {
      if (importer.getVerboseLevel() > 2)
        OLogManager.instance().info(this, "%s found record %s v%d for destination key %s", workerThread.getThreadId(),
            destinationVertex.getIdentity(), destinationVertex.getRecord().getVersion(), destinationVertexId);

      if (needReload) {
        destinationVertex.getRecord().reload(null, false);

        if (importer.getVerboseLevel() > 2)
          OLogManager.instance().info(this, "%s reloaded record %s v%d for source key %s", workerThread.getThreadId(),
              sourceVertex.getIdentity(), sourceVertex.getRecord().getVersion(), sourceVertexId);
      }
    }

    sourceVertex.addEdge(null, destinationVertex, edgeClassName, getThreadClusterName(graph, edgeClassName, sourceClusterIndex),
        properties);
  }
}
