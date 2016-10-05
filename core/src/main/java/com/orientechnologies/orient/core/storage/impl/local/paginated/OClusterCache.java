/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.storage.ORawBuffer;

/**
 * Cache that act at cluster level. It stores records with 2 arrays: byte[][] for the content and int[] for the versions.
 * 
 * @author Luca Garulli (l.garulli-at--orientdb.com)
 */
public class OClusterCache {
  private final int                    POINTER_RAM_SIZE   = 4;
  private final OClusterCacheCluster[] clusters           = new OClusterCacheCluster[32768];
  private final int                    maxChunks          = 16384;
  private final int                    chunkSize          = 131072;
  private long                         totalRecordContent = 0l;

  private class OClusterCacheCluster {
    public final OClusterCacheChunk[] chunks;

    public OClusterCacheCluster(final int chunks) {
      this.chunks = new OClusterCacheChunk[chunks];
    }
  }

  private class OClusterCacheChunk {
    public byte[][] records;
    public int[]    versions;
    public byte[]   recordTypes;
    public long     totalRecordContent = 0;

    public OClusterCacheChunk(final int chunkSize) {
      records = new byte[chunkSize][];
      versions = new int[chunkSize];
      recordTypes = new byte[chunkSize];
    }
  }

  public OClusterCache() {
  }

  public void shutdown() {
    free();
  }

  public synchronized ORawBuffer getRecord(final int clusterId, final long clusterPosition) {
    final OClusterCacheCluster cluster = clusters[clusterId];
    if (cluster == null)
      return null;

    final int chunkIndex = (int) (clusterPosition / maxChunks);

    if (chunkIndex >= cluster.chunks.length)
      return null;

    final OClusterCacheChunk chunk = cluster.chunks[chunkIndex];
    if (chunk == null)
      return null;

    final int contentIndex = (int) (clusterPosition % maxChunks);

    final byte[] content = chunk.records[contentIndex];
    if (content == null)
      return null;

    return new ORawBuffer(content, chunk.versions[contentIndex], chunk.recordTypes[contentIndex]);
  }

  public synchronized void setRecord(final int clusterId, final long clusterPosition, final byte[] content, final int version,
      final byte recordType) {
    OClusterCacheCluster cluster = clusters[clusterId];
    if (cluster == null) {
      cluster = new OClusterCacheCluster(maxChunks);
      clusters[clusterId] = cluster;
    }

    final int chunkIndex = (int) (clusterPosition / maxChunks);

    OClusterCacheChunk chunk = cluster.chunks[chunkIndex];
    if (chunk == null) {
      chunk = new OClusterCacheChunk(chunkSize);
      cluster.chunks[chunkIndex] = chunk;
    }

    final int contentIndex = (int) (clusterPosition % maxChunks);

    chunk.records[contentIndex] = content;
    chunk.versions[contentIndex] = version;
    chunk.recordTypes[contentIndex] = recordType;

    // UPDATE BOTH RAM STATS COUNTER
    chunk.totalRecordContent += content.length;
    totalRecordContent += content.length;
  }

  public synchronized void free() {
    for (int i = 0; i < clusters.length; ++i)
      freeCluster(i);
  }

  public synchronized void freeCluster(final int clusterId) {
    final OClusterCacheCluster cluster = clusters[clusterId];

    if (cluster != null) {
      for (OClusterCacheChunk chunk : cluster.chunks)
        totalRecordContent -= chunk.totalRecordContent;

      clusters[clusterId] = null;
    }
  }

  public synchronized void freeRecord(final int clusterId, final long clusterPosition) {
    final OClusterCacheCluster cluster = clusters[clusterId];
    if (cluster == null)
      return;

    final int chunkIndex = (int) (clusterPosition / maxChunks);

    if (chunkIndex >= cluster.chunks.length)
      return;

    final OClusterCacheChunk chunk = cluster.chunks[chunkIndex];
    if (chunk == null)
      return;

    final int contentIndex = (int) (clusterPosition % maxChunks);

    chunk.totalRecordContent -= chunk.records[contentIndex].length;
    totalRecordContent -= chunk.records[contentIndex].length;

    chunk.records[contentIndex] = null;
    chunk.versions[contentIndex] = 0;
    chunk.recordTypes[contentIndex] = 0;
  }

  public long getTotalContentSize() {
    return totalRecordContent;
  }

  public synchronized long getTotalRAMUsed() {
    long ram = totalRecordContent;

    ram += POINTER_RAM_SIZE * clusters.length;

    for (OClusterCacheCluster cluster : clusters) {
      if (cluster != null) {
        ram += POINTER_RAM_SIZE * cluster.chunks.length;

        for (OClusterCacheChunk chunk : cluster.chunks) {
          if (chunk != null) {
            ram += POINTER_RAM_SIZE * chunk.records.length;
            ram += 4 * chunk.versions.length; // INTEGERS = 4 BYTES
            ram += chunk.recordTypes.length; // BYTES
          }
        }
      }
    }

    return ram;
  }

}
