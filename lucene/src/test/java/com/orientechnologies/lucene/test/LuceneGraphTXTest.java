/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

/**
 * Created by enricorisa on 28/06/14.
 */

public class LuceneGraphTXTest {

  @Test
  public void graphTxTest() throws Exception {

    ODatabaseDocumentTx graph = new ODatabaseDocumentTx("memory:graphTx");
    graph.create();

    try {

      OSchema schema = graph.getMetadata().getSchema();
      OClass city = schema.createClass("City", schema.getClass("V"));
      city.createProperty("name", OType.STRING);
      graph.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();

      graph.begin();
      OVertex v = graph.newVertex("City");
      v.setProperty("name", "London");

      v.save();

      Collection results = graph.command(new OCommandSQL("select from City where name lucene 'London'")).execute();
      Assert.assertEquals(results.size(), 1);

      v.setProperty("name", "Berlin");

      v.save();

      results = graph.command(new OCommandSQL("select from City where name lucene 'Berlin'")).execute();
      Assert.assertEquals(results.size(), 1);

      results = graph.command(new OCommandSQL("select from City where name lucene 'London'")).execute();
      Assert.assertEquals(results.size(), 0);

      graph.commit();

      // Assert After Commit
      results = graph.command(new OCommandSQL("select from City where name lucene 'Berlin'")).execute();
      Assert.assertEquals(results.size(), 1);
      results = graph.command(new OCommandSQL("select from City where name lucene 'London'")).execute();
      Assert.assertEquals(results.size(), 0);

    } finally {
      graph.drop();
    }

  }

}
