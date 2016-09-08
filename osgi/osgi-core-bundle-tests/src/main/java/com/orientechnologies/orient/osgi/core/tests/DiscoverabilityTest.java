/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.osgi.core.tests;

import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import org.apache.felix.ipojo.junit4osgi.OSGiTestCase;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

/**
 * @author Sergey Sitnikov
 */
public class DiscoverabilityTest extends OSGiTestCase {

  public void testAllEnginesAreAvailable()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException, NotCompliantMBeanException,
      MBeanRegistrationException, InvocationTargetException, NoSuchMethodException, InstantiationException, IOException,
      IllegalAccessException {

    final Set<String> engines = Orient.instance().getEngines();

    assertTrue(engines.contains(OEngineMemory.NAME));
    assertTrue(engines.contains(OEngineLocalPaginated.NAME));
    assertTrue(engines.contains(OEngineRemote.NAME));

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:DiscoverabilityTest");
    db.create();
    new ODocument().field("key", "value").save();
    db.drop();

    db = new ODatabaseDocumentTx("plocal:./DiscoverabilityTest");
    db.create();
    new ODocument().field("key", "value").save();
    db.drop();

    System.setProperty("ORIENTDB_HOME", "./target/server");
    final OServer server = new OServer(false);
    server.startup(DiscoverabilityTest.class.getClassLoader().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
    new OServerAdmin("remote:localhost").connect("root", "root").createDatabase("DiscoverabilityTest", "document", "plocal")
        .close();
    db = new ODatabaseDocumentTx("remote:localhost/DiscoverabilityTest");
    db.open("root", "root");
    new ODocument().field("key", "value").save();
    new OServerAdmin("remote:localhost").connect("root", "root").dropDatabase("DiscoverabilityTest", "document").close();
    server.shutdown();
  }

}
