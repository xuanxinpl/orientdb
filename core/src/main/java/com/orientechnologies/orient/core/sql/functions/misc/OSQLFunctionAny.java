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
package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.query.OQueryRuntimeValueMulti;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a date object from the format passed. If no arguments are passed, than the system date is built (like sysdate() function)
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @see com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionSysdate
 *
 */
public class OSQLFunctionAny extends OSQLFunctionAbstract {
  public static final String NAME = "any";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public OSQLFunctionAny() {
    super(NAME, 0, 0);
  }

  public Object execute(Object iThis, final OIdentifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
      OCommandContext iContext) {

    List<Object> values = new ArrayList<Object>();
    if(iThis instanceof  OIdentifiable){
      ODocument doc = ((OIdentifiable) iThis).getRecord();
      for(String field:doc.fieldNames()){
        values.add(doc.field(field));
      }
    }
    return new OQueryRuntimeValueMulti(null, values.toArray(), null);
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }

  public String getSyntax() {
    return "any()";
  }

  @Override
  public Object getResult() {
    return null;
  }
}
