/* Generated By:JJTree: Do not edit this line. OMatchPathItem.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class OMatchPathItem extends SimpleNode {
  protected OMethodCall  method;
  protected OMatchFilter filter;

  public OMatchPathItem(int id) {
    super(id);
  }

  public OMatchPathItem(OrientSql p, int id) {
    super(p, id);
  }

  /**
   * Accept the visitor.
   **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  public boolean isBidirectional() {
    if (filter.getWhileCondition() != null) {
      return false;
    }
    if (filter.getMaxDepth() != null) {
      return false;
    }
    if (filter.isOptional()) {
      return false;
    }
    return method.isBidirectional();
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    method.toString(params, builder);
    if (filter != null) {
      filter.toString(params, builder);
    }
  }

  protected Iterable<OIdentifiable> executeTraversal(final OMatchStatement.MatchContext matchContext,
      final OCommandContext iCommandContext, final OIdentifiable startingPoint, int depth) {
    return new Iterable<OIdentifiable>() {
      @Override public Iterator<OIdentifiable> iterator() {
        return new OMatchPathItemIterator(OMatchPathItem.this, matchContext, iCommandContext, startingPoint);
      }
    };
  }

  protected boolean matchesClass(OIdentifiable identifiable, OClass oClass) {
    if (identifiable == null) {
      return false;
    }
    ORecord record = identifiable.getRecord();
    if (record == null) {
      return false;
    }
    if (record instanceof ODocument) {
      return ((ODocument) record).getSchemaClass().isSubClassOf(oClass);
    }
    return false;
  }

  protected Iterable<OIdentifiable> traversePatternEdge(OMatchStatement.MatchContext matchContext, OIdentifiable startingPoint,
      OCommandContext iCommandContext) {

    Iterable possibleResults = null;
    if (filter != null) {
      OIdentifiable matchedNode = matchContext.matched.get(filter.getAlias());
      if (matchedNode != null) {
        possibleResults = Collections.singleton(matchedNode);
      } else if (matchContext.matched.containsKey(filter.getAlias())) {
        possibleResults = Collections.emptySet();//optional node, the matched element is a null value
      } else {
        possibleResults = matchContext.candidates == null ? null : matchContext.candidates.get(filter.getAlias());
      }
    }

    Object qR = this.method.execute(startingPoint, possibleResults, iCommandContext);
    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton(qR);
  }
}
/* JavaCC - OriginalChecksum=ffe8e0ffde583d7b21c9084eff6a8944 (do not edit this line) */
