package com.orientechnologies.lucene.engine;

import com.orientechnologies.lucene.collections.OLuceneAbstractResultSet;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndexCursor;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by Enrico Risa on 16/01/17.
 */
public class OLuceneIndexCursor implements OIndexCursor {

  private final Object                   key;
  private       OLuceneAbstractResultSet resultSet;

  private Iterator<OIdentifiable> iterator;

  public OLuceneIndexCursor(OLuceneAbstractResultSet resultSet, Object key) {
    this.resultSet = resultSet;
    this.iterator = resultSet.iterator();
    this.key = key;
  }

  @Override
  public Map.Entry<Object, OIdentifiable> nextEntry() {

    if (iterator.hasNext()) {
      final OIdentifiable next = iterator.next();
      return new Map.Entry<Object, OIdentifiable>() {
        @Override
        public Object getKey() {
          return key;
        }

        @Override
        public OIdentifiable getValue() {
          return next;
        }

        @Override
        public OIdentifiable setValue(OIdentifiable value) {
          return null;
        }
      };
    }
    return null;
  }

  @Override
  public Set<OIdentifiable> toValues() {
    return null;
  }

  @Override
  public Set<Map.Entry<Object, OIdentifiable>> toEntries() {
    return null;
  }

  @Override
  public Set<Object> toKeys() {
    return null;
  }

  @Override
  public void setPrefetchSize(int prefetchSize) {

  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public OIdentifiable next() {
    return iterator.next();
  }

  @Override
  public void remove() {

  }
}
