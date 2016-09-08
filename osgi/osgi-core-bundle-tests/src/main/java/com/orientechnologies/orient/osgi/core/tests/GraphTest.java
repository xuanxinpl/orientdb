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

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.apache.felix.ipojo.junit4osgi.OSGiTestCase;

/**
 * @author Sergey Sitnikov
 */
public class GraphTest extends OSGiTestCase {

  public void testBlueprints() {
    final OrientGraphFactory factory = new OrientGraphFactory("memory:GraphTest");
    final Graph graph = factory.getTx();
    final Vertex vertex1 = graph.addVertex("id1");
    final Vertex vertex2 = graph.addVertex("id2");
    final Edge edge = vertex1.addEdge("label", vertex2);

    vertex1.setProperty("key", "value");
    edge.setProperty("key", "value");
  }

  public void testOrientBlueprints() {
    final OrientGraphFactory factory = new OrientGraphFactory("memory:GraphTest");
    final OrientGraph graph = factory.getTx();
    final OrientVertex vertex1 = graph.addVertex("id1");
    final OrientVertex vertex2 = graph.addVertex("id2");
    final OrientEdge edge = (OrientEdge) vertex1.addEdge("label", vertex2);

    vertex1.setProperty("key", "value");
    edge.setProperty("key", "value");
  }

}
