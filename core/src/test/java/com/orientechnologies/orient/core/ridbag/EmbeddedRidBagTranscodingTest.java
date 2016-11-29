/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.ridbag;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author Sergey Sitnikov
 */
public class EmbeddedRidBagTranscodingTest {

  private ODatabaseDocumentTx db;
  private ORidBag sourceBag;


  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + EmbeddedRidBagTranscodingTest.class.getSimpleName());
    db.create();

    sourceBag = new ORidBag();
    sourceBag.setAutoConvertToRecord(false);
    sourceBag.add(new ORecordId(1, 1));
    sourceBag.add(new ORecordId(2, 22));
    sourceBag.add(new ORecordId(3, 333));
    sourceBag.add(new ORecordId(4, 4444));
    sourceBag.add(new ORecordId(5, 55555));
  }

  @Test
  public void test() {
    final ORidBag originalBag = new ORidBag();
    originalBag.setAutoConvertToRecord(false);
    final BytesContainer originalBytes = new BytesContainer();
    sourceBag.toStream(originalBytes, ORidBag.Encoding.Original);
    originalBytes.offset = 0;
    originalBag.fromStream(originalBytes);
    originalBag.add(new ORecordId(6, 666666));

    final ORidBag compactBag = new ORidBag();
    originalBag.setAutoConvertToRecord(false);
    final BytesContainer compactBytes = new BytesContainer();
    originalBag.toStream(compactBytes, ORidBag.Encoding.Compact);
    compactBytes.offset = 0;
    compactBag.fromStream(compactBytes);
    compactBag.add(new ORecordId(7, 7777777));

    for (OIdentifiable i : sourceBag)
      assertTrue(originalBag.contains(i));
    assertTrue(originalBag.contains(new ORecordId(6, 666666)));

    for (OIdentifiable i : originalBag)
      assertTrue(compactBag.contains(i));
    assertTrue(compactBag.contains(new ORecordId(7, 7777777)));
  }

  @After
  public void after() {
    db.drop();
  }

}
