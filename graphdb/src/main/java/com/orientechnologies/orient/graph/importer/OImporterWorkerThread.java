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

import java.util.concurrent.ArrayBlockingQueue;

public class OImporterWorkerThread extends Thread {
  private final OGraphImporter                importer;
  private final String                        sourceClassName;
  private final int                           sourceClusterIndex;
  private final String                        destinationClassName;
  private final int                           destinationClusterIndex;
  private final String                        edgeClassName;

  private final ArrayBlockingQueue<Operation> queue;

  private long                                localTotalRetry     = 0;
  private long                                localOperationCount = 0;

  private String                              sourceClusterName;
  private String                              destinationClusterName;
  private String                              edgeClusterName;

  public OImporterWorkerThread(final OGraphImporter importer, final String sourceClassName, final int sourceClusterIndex,
      final String destinationClassName, final int destinationClusterIndex, final String edgeClassName) {
    this.importer = importer;
    this.sourceClassName = sourceClassName;
    this.sourceClusterIndex = sourceClusterIndex;
    this.destinationClassName = destinationClassName;
    this.destinationClusterIndex = destinationClusterIndex;
    this.edgeClassName = edgeClassName;
    this.queue = new ArrayBlockingQueue<Operation>(importer.getQueueSize());
  }

  public OImporterWorkerThread(final OGraphImporter importer, final String vertexClassName, final int vertexClusterIndex) {
    this.importer = importer;
    this.sourceClassName = vertexClassName;
    this.sourceClusterIndex = vertexClusterIndex;
    this.destinationClassName = null;
    this.destinationClusterIndex = 0;
    this.edgeClassName = null;
    this.queue = new ArrayBlockingQueue<Operation>(importer.getQueueSize());
  }

  public void sendOperation(final Operation operation) throws InterruptedException {
    queue.put(operation);
  }

  @Override
  public void run() {
    final OrientBaseGraph graph = importer.isTransactional() ? importer.getFactory().getTx() : importer.getFactory().getNoTx();
    try {
      sourceClusterName = OAbstractBaseOperation.getThreadClusterName(graph, sourceClassName, sourceClusterIndex);
      if (destinationClassName != null)
        destinationClusterName = OAbstractBaseOperation.getThreadClusterName(graph, destinationClassName, destinationClusterIndex);
      if (edgeClassName != null)
        edgeClusterName = OAbstractBaseOperation.getThreadClusterName(graph, edgeClassName, sourceClusterIndex);

      final int batchSize = importer.getBatchSize();

      while (true) {
        try {
          final Operation operation = queue.take();

          if (operation instanceof OEndOperation)
            // END
            break;

          operation.execute(importer, graph, sourceClusterIndex, destinationClusterIndex);

          localOperationCount++;

          if (batchSize > 0 && localOperationCount % batchSize == 0) {
            // COMMIT THE BATCH
            commit(graph);
          }

        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      commit(graph);

    } catch (Throwable t) {
      OLogManager.instance().error(this, "Error while processing next operation", t);
    } finally {
      graph.shutdown();
    }
  }

  protected void commit(final OrientBaseGraph graph) {
    importer.lockClusters(sourceClusterName, destinationClusterName, edgeClusterName);
    try {
      graph.commit();
    } finally {
      importer.unlockClusters(sourceClusterName, destinationClusterName, edgeClusterName);
      importer.unlockCreationCurrentThread();
    }
  }

  public String getStats() {
    return String.format("total=%d retry=%d", localOperationCount, localTotalRetry);
  }
}
