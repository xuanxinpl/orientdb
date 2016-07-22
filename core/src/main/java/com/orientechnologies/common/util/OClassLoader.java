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

package com.orientechnologies.common.util;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides extensible centralized service for {@link Class class} and {@link ServiceLoader service} loading.
 */
public class OClassLoader {

  private static final Set<ClassLoader>      classLoaders = Collections
      .newSetFromMap(new ConcurrentHashMap<ClassLoader, Boolean>());
  private static final Map<String, Class<?>> cache        = new ConcurrentHashMap<String, Class<?>>(128);

  private OClassLoader() {
  }

  /**
   * Does the same thing as {@link Class#forName(String)} consulting class loaders known to {@link OClassLoader}.
   *
   * @param className the fully qualified name of the desired class.
   *
   * @return the {@code Class} object for the class with the specified name.
   *
   * @throws ClassNotFoundException if class not found in any of known class loaders.
   * @see #registerClassLoader(ClassLoader)
   */
  public static Class<?> classForName(String className) throws ClassNotFoundException {

    // Fast path.

    if (classLoaders.isEmpty())
      return Class.forName(className);

    // Inspect the cache.

    Class<?> class_ = cache.get(className);

    ClassNotFoundException classNotFoundException = null;
    if (class_ == null || class_ == NotFoundClass.class) {

      // Try default loading mechanics first and store ClassNotFoundException if any.

      try {
        return Class.forName(className);
      } catch (ClassNotFoundException e) {
        classNotFoundException = e;
      }
    }

    if (class_ == null) {

      // Try thread context class loader for unseen classes.

      final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      if (contextClassLoader != null)
        try {
          return contextClassLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
          // try harder
        }

      // Try system class loader for unseen classes.

      final ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
      if (systemClassLoader != null)
        try {
          return systemClassLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
          // try harder
        }

      // Try registered class loaders for unseen classes and cache the result.

      class_ = NotFoundClass.class;

      for (ClassLoader classLoader : classLoaders)
        try {
          class_ = classLoader.loadClass(className);
          break;
        } catch (ClassNotFoundException e) {
          // try next class loader
        }

      cache.put(className, class_);
    }

    if (class_ == NotFoundClass.class)
      throw classNotFoundException;

    return class_;
  }

  /**
   * Does the same thing as {@link ServiceLoader#load(Class)} consulting class loaders known to {@link OClassLoader}.
   *
   * @param class_ the interface or abstract class representing the service.
   * @param <T>    the class of the service type.
   *
   * @return the iterator with service instances, never {@code null}.
   *
   * @see #registerClassLoader(ClassLoader)
   */
  public static <T> Iterator<T> loadService(Class<T> class_) {
    try {

      // Fast path.

      if (classLoaders.isEmpty())
        return ServiceLoader.load(class_).iterator();

      // Try registered class loaders.

      final List<T> services = new ArrayList<T>();
      for (ClassLoader classLoader : classLoaders)
        for (T service : ServiceLoader.load(class_, classLoader))
          services.add(service);

      // Try OrientDB core class loader.

      final ClassLoader coreClassLoader = OClassLoader.class.getClassLoader();
      if (!classLoaders.contains(coreClassLoader))
        for (T service : ServiceLoader.load(class_, coreClassLoader))
          services.add(service);

      // Try thread context class loader which is a default behavior for ServiceLoader.

      final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      if (contextClassLoader != null && contextClassLoader != coreClassLoader && !classLoaders.contains(contextClassLoader))
        for (T service : ServiceLoader.load(class_, contextClassLoader))
          services.add(service);

      // Try system class loader.

      final ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
      if (systemClassLoader != null && systemClassLoader != coreClassLoader && systemClassLoader != contextClassLoader
          && !classLoaders.contains(systemClassLoader))
        for (T service : ServiceLoader.load(class_, systemClassLoader))
          services.add(service);

      // Done.

      return services.iterator();
    } catch (Exception e) {
      OLogManager.instance().warn(null, "Cannot load service", e);
      throw OException.wrapException(new OConfigurationException("Cannot load service"), e);
    }
  }

  /**
   * Registers the given class loader in {@link OClassLoader}.
   *
   * @param classLoader the class loader to register.
   */
  public static void registerClassLoader(ClassLoader classLoader) {
    classLoaders.add(classLoader);
  }

  /**
   * Unregisters the given class loader in {@link OClassLoader}.
   *
   * @param classLoader the class loader to unregister.
   */
  public static void unregisterClassLoader(ClassLoader classLoader) {
    classLoaders.remove(classLoader);
  }

  private static class NotFoundClass {
  }

}
