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

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by enricorisa on 03/09/14.
 */
public class GraphEmbeddedTest extends BaseLuceneTest {

  public GraphEmbeddedTest() {

  }

  @Before
  public void init() {

    OSchema schema = db.getMetadata().getSchema();

    OClass type = schema.createClass("City", schema.getClass("V"));
    type.createProperty("latitude", OType.DOUBLE);
    type.createProperty("longitude", OType.DOUBLE);
    type.createProperty("name", OType.STRING);

    db.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();
  }

  @Test
  public void embeddedTx() {

    //THIS WON'T USE LUCENE INDEXES!!!! see #6997

    db.begin();
    OVertex vertex = db.newVertex("City");
    vertex.setProperty("name", "London / a");
    vertex.save();

    vertex = db.newVertex("City");
    vertex.setProperty("name", "Rome");
    vertex.save();
    db.commit();

    OResultSet rs = db.query("select from City where name=  \"London\"");
    long size = rs.vertexStream().count();
    Assert.assertEquals(size, 1L);

    size = (int) db.query("select from City where name=  \"Rome\"").vertexStream().count();

    Assert.assertEquals(size, 1L);
  }

  @Test
  public void testGetVerticesFilterClass() {

    OSchema schema = db.getMetadata().getSchema();

    OClass vClass = schema.getClass("V");
    schema.createClass("One", schema.getClass("V"));
    schema.createClass("Two", schema.getClass("V"));

    vClass.createProperty("name",OType.STRING);
    vClass.createIndex("V.name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    db.begin();
    OVertex v = db.newVertex("One");
    v.setProperty("name", "Same" );
    v.save();
    v = db.newVertex("Two");
    v.setProperty("name", "Same" );
    v.save();

    db.commit();

    long size = db.query("select from One where name= \"Same\"").vertexStream().count();

    Assert.assertEquals(1L, size);
  }

}
