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

/**
 *
 * @author Luca Garulli (l.garulli-(at)-orientdb.com)
 */
public class OCreateVertexOperation extends OAbstractBaseOperation {
  private final String              className;
  private final Object              id;
  private final Map<String, Object> properties;

  public OCreateVertexOperation(final String className, final Object id, final Map<String, Object> properties) {
    this.className = className;
    this.id = id;
    this.properties = properties;
  }

  public boolean execute(final OGraphImporter importer, OImporterWorkerThread workerThread, final OrientBaseGraph graph,
      final int sourceClusterIndex, int destinationClusterIndex) {
    OrientVertex v = lookupVertex(importer, graph, className, id);

    if (v != null)
      // JUST MERGE PROPERTIES
      v.setProperties(properties);
    else {
      // WAIT TO LOCK THE KEY
      if (!importer.lockVertexCreationByKey(workerThread, graph, className, id,
          attempts > importer.getMaxAttemptsToFlushTransaction()))
        // POSTPONE EXECUTION
        return false;

      v = graph.addTemporaryVertex(className, properties);
      v.getRecord().field(importer.getRegisteredVertexClass(className).getKey(), id);
      v.save(getThreadClusterName(graph, className, sourceClusterIndex));
    }
    return true;
  }
}