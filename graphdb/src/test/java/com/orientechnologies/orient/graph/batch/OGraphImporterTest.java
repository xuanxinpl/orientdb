package com.orientechnologies.orient.graph.batch;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.graph.importer.OGraphImporter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Luca Garulli
 */

public class OGraphImporterTest {

  public static void main(String[] args) throws IOException, InterruptedException {
    // String dbUrl = "memory:amazonReviews";
    String dbUrl = "plocal:/temp/databases/amazonReviews";

    final File f = new File("/temp/databases/amazonReviews");
    if (f.exists())
      OFileUtils.deleteRecursively(f);

    OGraphImporter batch = new OGraphImporter(dbUrl, "admin", "admin");
    batch.setTotalEstimatedEdges(22507155);

    batch.setTransactional(false);
    batch.setParallel(4);
    batch.setBatchSize(10);
    batch.setLightweightEdges(false);

    batch.setEnableClusterLocking(false);
    batch.setEnableVertexLocking(false);

    batch.setVerboseLevel(1);
    batch.setQueueSize(2000);
    batch.setMaxAttemptsToFlushTransaction(3);
    batch.setBackPressureThreshold(2000);

    batch.registerVertexClass("User", "id", OType.STRING);
    batch.registerVertexClass("Product", "id", OType.STRING);
    batch.registerEdgeClass("Reviewed", "User", "Product");

    batch.begin();

    final File file = new File("/Users/luca/Downloads/ratings_Books.csv");
    final BufferedReader br = new BufferedReader(new FileReader(file));

    try {
      int row = 0;
      for (String line; (line = br.readLine()) != null;) {
        row++;

        final String[] parts = line.split(",");

        if (parts.length != 4) {
          // SKIP IT
          System.out.print("Skipped invalid line " + row + ": " + line);
          continue;
        }

        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("score", new Float(parts[2]).intValue());
        properties.put("date", Long.parseLong(parts[3]));

        batch.createEdge("Reviewed", "User", parts[0], "Product", parts[1], properties);
      }
    } finally {
      br.close();
    }

    batch.end();
  }
}
