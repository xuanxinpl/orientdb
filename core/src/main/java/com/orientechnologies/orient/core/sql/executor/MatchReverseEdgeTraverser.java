package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;

import java.util.Collections;

/**
 * Created by luigidellaquila on 15/10/16.
 */
public class MatchReverseEdgeTraverser extends MatchEdgeTraverser {

  private final String startingPointAlias;
  private final String endPointAlias;

  public MatchReverseEdgeTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
    this.startingPointAlias = edge.edge.in.alias;
    this.endPointAlias = edge.edge.out.alias;
  }

  protected void init(OCommandContext ctx) {
    if (downstream == null) {
      Object startingElem = sourceRecord.getProperty(getStartingPointAlias());
      if (startingElem instanceof OResult) {
        startingElem = ((OResult) startingElem).getElement().orElse(null);
      }

      String targetClass = edge.edge.out.getTargetClass();
      OWhereClause whereCond = edge.edge.out.getFilter();
      OWhereClause whileCond = null;
      Integer maxDepth = null;

      downstream = executeTraversal(ctx, targetClass, whereCond, whileCond, maxDepth, (OIdentifiable) startingElem, 0).iterator();
    }
  }

  @Override protected Iterable<OIdentifiable> traversePatternEdge(OIdentifiable startingPoint, OCommandContext iCommandContext) {

    Object qR = this.item.getMethod().executeReverse(startingPoint, iCommandContext);
    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((OIdentifiable) qR);
  }

  @Override protected String getStartingPointAlias() {
    return this.startingPointAlias;
  }

  @Override protected String getEndpointAlias() {
    return endPointAlias;
  }

  protected boolean matchesFilters(OCommandContext iCommandContext, OWhereClause filter, OIdentifiable origin) {
    return true;
  }

}
