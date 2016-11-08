package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by luigidellaquila on 28/07/15.
 */
public class PatternNode {
  public String alias;
  public Set<PatternEdge> out        = new LinkedHashSet<PatternEdge>();
  public Set<PatternEdge> in         = new LinkedHashSet<PatternEdge>();
  public int              centrality = 0;
  public boolean          optional   = false;
  private OWhereClause filter;
  private String       targetClass;

  public PatternNode() {

  }

  public int addEdge(OMatchPathItem item, PatternNode to) {
    PatternEdge edge = new PatternEdge();
    edge.item = item;
    edge.out = this;
    edge.in = to;
    this.out.add(edge);
    to.in.add(edge);
    return 1;
  }

  public boolean isOptionalNode() {
    return optional;
  }

  public void setFilter(OWhereClause filter) {
    this.filter = filter;
  }

  public OWhereClause getFilter() {
    return filter;
  }

  public void setTargetClass(String targetClass) {
    this.targetClass = targetClass;
  }

  public String getTargetClass() {
    return targetClass;
  }
}
