/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * OrientDB LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * OrientDB LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from OrientDB LTD.
 * 
 * For more information: http://www.orientdb.com
 */

package com.mdm.factory;

import com.mdm.context.OTeleporterContext;
import com.mdm.nameresolver.OJavaConventionNameResolver;
import com.mdm.nameresolver.ONameResolver;
import com.mdm.nameresolver.OOriginalConventionNameResolver;

/**
 * Factory used to instantiate a specific NameResolver starting from its name.
 * If the name is not specified (null value) a JavaConventionNameResolver is instantiated.
 *  
 * @author Gabriele Ponzi
 * @email  <g.ponzi--at--orientdb.com>
 *
 */

public class ONameResolverFactory {

  public ONameResolver buildNameResolver(String nameResolverConvention) {
    ONameResolver nameResolver = null;


    if(nameResolverConvention == null)  {
      nameResolver = new OOriginalConventionNameResolver();
    }
    else {
      switch(nameResolverConvention) {

      case "java": nameResolver = new OJavaConventionNameResolver();
      break;

      case "original": nameResolver = new OOriginalConventionNameResolver();
      break;

      default : nameResolver = new OOriginalConventionNameResolver();
        OTeleporterContext.getInstance().getStatistics().warningMessages.add("Name resolver convention '" + nameResolverConvention + "' not found, the original name convention will be adopted.");
      break;

      }
    }

    OTeleporterContext.getInstance().setNameResolver(nameResolver);

    return nameResolver;
  }

}
