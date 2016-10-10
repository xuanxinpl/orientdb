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

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Worker thread that is run in parallel. There is one working thread per combination ov vertex-edge->vertex.
 *
 * @author Luca Garulli (l.garulli-(at)-orientdb.com)
 */
public class OImporterWorkerThread extends Thread {
  private final OGraphImporter                    importer;
  private final String                            sourceClassName;
  private final int                               sourceClusterIndex;
  private final String                            destinationClassName;
  private final int                               destinationClusterIndex;
  private final String                            edgeClassName;

  private final ArrayBlockingQueue<OOperation>    queue;
  private final ConcurrentLinkedQueue<OOperation> priorityQueue;
  private final ArrayList<OOperation>             executedOperationInTx;

  private long                                    localTotalRetry         = 0;
  private long                                    localOperationCount     = 0;
  private long                                    localOperationInBatch   = 0;
  private long                                    localEdgeCreatedInBatch = 0;

  private String                                  sourceClusterName;
  private String                                  destinationClusterName;
  private String                                  edgeClusterName;
  private String                                  id;
  private boolean                                 retryInProgress         = false;

  public OImporterWorkerThread(final String id, final OGraphImporter importer, final String sourceClassName,
      final int sourceClusterIndex, final String destinationClassName, final int destinationClusterIndex,
      final String edgeClassName) {
    setName("OGraphImporter-WT-" + id);
    this.id = id;
    this.importer = importer;
    this.sourceClassName = sourceClassName;
    this.sourceClusterIndex = sourceClusterIndex;
    this.destinationClassName = destinationClassName;
    this.destinationClusterIndex = destinationClusterIndex;
    this.edgeClassName = edgeClassName;
    this.queue = new ArrayBlockingQueue<OOperation>(importer.getQueueSize());
    this.priorityQueue = new ConcurrentLinkedQueue<OOperation>();
    this.executedOperationInTx = new ArrayList<OOperation>(importer.getBatchSize());
  }

  public OImporterWorkerThread(final String id, final OGraphImporter importer, final String vertexClassName,
      final int vertexClusterIndex) {
    setName("OGraphImporter-WT-" + id);
    this.id = id;
    this.importer = importer;
    this.sourceClassName = vertexClassName;
    this.sourceClusterIndex = vertexClusterIndex;
    this.destinationClassName = null;
    this.destinationClusterIndex = 0;
    this.edgeClassName = null;
    this.queue = new ArrayBlockingQueue<OOperation>(importer.getQueueSize());
    this.priorityQueue = new ConcurrentLinkedQueue<OOperation>();
    this.executedOperationInTx = new ArrayList<OOperation>(importer.getBatchSize());
  }

  public void sendOperation(final OOperation operation) throws InterruptedException {
    queue.put(operation);
  }

  public void sendPriorityOperation(final OOperation operation) {
    priorityQueue.offer(operation);
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

      final ArrayBlockingQueue<OOperation> operationToReExecute = new ArrayBlockingQueue<OOperation>(importer.getBatchSize());

      while (true) {
        try {
          OOperation operation = null;

          if (retryInProgress) {
            operation = operationToReExecute.poll();
            if (operation == null)
              // RETRY FINISHED
              retryInProgress = false;
          }

          if (operation == null) {
            operation = priorityQueue.poll();
            if (operation == null) {
              operation = queue.poll();
              if (operation == null) {
                Thread.sleep(1);
                continue;
              }
            }
          }

          if (operation instanceof OEndOperation)
            // END
            break;

          if (!(operation instanceof OCommitOperation))
            executedOperationInTx.add(operation);

          operation.execute(importer, this, graph, sourceClusterIndex, destinationClusterIndex);

          if (!(operation instanceof OCommitOperation)) {
            localOperationCount++;
            localOperationInBatch++;

            if (batchSize > 0 && localOperationInBatch >= batchSize) {
              // COMMIT THE BATCH
              commit(graph);
            }
          }

        } catch (ONeedRetryException e) {

          prepareRedoOperations(operationToReExecute);

        } catch (ORecordDuplicatedException e) {

          prepareRedoOperations(operationToReExecute);
        }
      }

      commit(graph);

    } catch (Throwable t) {
      OLogManager.instance().error(this, "Error while processing next operation", t);
    } finally {
      graph.shutdown();
    }
  }

  public void commit(final OrientBaseGraph graph) {
    try {
      if (localOperationInBatch == 0)
        return;

      importer.lockClustersForCommit(sourceClusterName, destinationClusterName, edgeClusterName);
      try {

        localOperationInBatch = 0;

        graph.commit();

        executedOperationInTx.clear();

        importer.addTotalEdges(localEdgeCreatedInBatch);
        localEdgeCreatedInBatch = 0;

      } finally {

        importer.unlockClustersForCommit(sourceClusterName, destinationClusterName, edgeClusterName);
        graph.getRawGraph().getLocalCache().clear();
      }

    } finally {
      importer.unlockCreationCurrentThread(id);
    }
  }

  public String getThreadId() {
    return id;
  }

  public int getOperationsInQueue() {
    return queue.size();
  }

  public long getRetries() {
    return localTotalRetry;
  }

  public void incrementLocalEdgeCreatedInBatch() {
    localEdgeCreatedInBatch++;
  }

  protected void prepareRedoOperations(final ArrayBlockingQueue<OOperation> operationToReExecute) {
    // TRANSFER THE PENDING OPERATIONS IN THE QUEUE TO BE RE-EXECUTED
    retryInProgress = true;
    localTotalRetry++;
    for (OOperation op : executedOperationInTx)
      operationToReExecute.offer(op);
    executedOperationInTx.clear();
  }
}
