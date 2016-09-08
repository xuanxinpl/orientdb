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

package com.orientechnologies.orient.osgi.core;

import com.orientechnologies.common.util.OClassLoader;
import com.orientechnologies.orient.core.Orient;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

/**
 * @author Sergey Sitnikov
 */
public class OOsgiCoreBundleActivator implements BundleActivator {

  @Override
  public void start(BundleContext context) throws Exception {
    OClassLoader.registerClassLoader(OOsgiCoreBundleActivator.class.getClassLoader());
  }

  @Override
  public void stop(BundleContext context) throws Exception {
    try {
      // We cannot rely on shutdown hooks in OSGi environment, since they may be invoked on a thread that
      // has non-OSGi class loader, this may produce various glitches, like unexpected IllegalAccessError.
      // As a countermeasure, shutdown OrientDB from here.
      Orient.instance().shutdown();
    } finally {
      OClassLoader.unregisterClassLoader(OOsgiCoreBundleActivator.class.getClassLoader());
    }
  }

}
