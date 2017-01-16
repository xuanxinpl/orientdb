package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 18/11/2016.
 */
public class LucenePhraseQueriesTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {

    OClass type = db.getMetadata().getSchema().createClass("Role", db.getMetadata().getSchema().getClass("V"));
    type.createProperty("name", OType.STRING);

    db.command(new OCommandSQL("create index Role.name on Role (name) FULLTEXT ENGINE LUCENE " + "METADATA{"
        + "\"name_index\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\"," + "\"name_index_stopwords\": \"[]\","
        + "\"name_query\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\"," + "\"name_query_stopwords\": \"[]\""
        //                + "\"name_query\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\""
        + "} ")).execute();

    OVertex v = db.newVertex("Role");
    v.setProperty("name", "System IT Owner");
    v.save();
    v = db.newVertex("Role");
    v.setProperty("name", "System Business Owner");
    v.save();
    v = db.newVertex("Role");
    v.setProperty("name", "System Business SME");
    v.save();
    v = db.newVertex("Role");
    v.setProperty("name", "System Technical SME");
    v.save();
    v = db.newVertex("Role");
    v.setProperty("name", "System");
    v.save();
    v = db.newVertex("Role");
    v.setProperty("name", "boat");
    v.save();
    v = db.newVertex("Role");
    v.setProperty("name", "moat");
    v.save();

  }

  @Test
  public void testPhraseQueries() throws Exception {

    OResultSet vertexes = db.command("select from Role where name lucene ' \"Business Owner\" '  ");

    Assert.assertEquals(1L, vertexes.stream().count());

    vertexes = db.command("select from Role where name lucene ' \"Owner of Business\" '  ");

    Assert.assertEquals(0L, vertexes.stream().count());

    vertexes = db.command("select from Role where name lucene ' \"System Owner\" '  ");

    Assert.assertEquals(0L, vertexes.stream().count());

    vertexes = db.command("select from Role where name lucene ' \"System SME\"~1 '  ");

    Assert.assertEquals(2L, vertexes.stream().count());

    vertexes = db.command("select from Role where name lucene ' \"System Business\"~1 '  ");

    Assert.assertEquals(2L, vertexes.stream().count());

    vertexes = db.command("select from Role where name lucene ' /[mb]oat/ '  ");

    Assert.assertEquals(2L, vertexes.stream().count());

  }

  @Test
  public void testComplexPhraseQueries() throws Exception {

    Iterable<OElement> vertexes = db.command(new OCommandSQL("select from Role where name lucene ?")).execute("\"System SME\"~1");

    assertThat(vertexes).allMatch(v -> v.<String>getProperty("name").contains("SME"));

    vertexes = db.command(new OCommandSQL("select from Role where name lucene ? ")).execute("\"SME System\"~1");

    assertThat(vertexes).isEmpty();

    vertexes = db.command(new OCommandSQL("select from Role where name lucene ? ")).execute("\"Owner Of Business\"");
    vertexes.forEach(v -> System.out.println("v = " + v.getProperty("name")));

    assertThat(vertexes).isEmpty();

    vertexes = db.command(new OCommandSQL("select from Role where name lucene ? ")).execute("\"System Business SME\"");

    assertThat(vertexes).hasSize(1).allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business SME"));

    vertexes = db.command(new OCommandSQL("select from Role where name lucene ? ")).execute("\"System Owner\"~1 -business");

    assertThat(vertexes).hasSize(1).allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System IT Owner"));

    vertexes = db.command(new OCommandSQL("select from Role where name lucene ? ")).execute("\"System Owner\"~1 -IT");
    assertThat(vertexes).hasSize(1).allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business Owner"));

    vertexes = db.command(new OCommandSQL("select from Role where name lucene ? ")).execute("+System +Own*~0.0 -IT");
    assertThat(vertexes).hasSize(1).allMatch(v -> v.<String>getProperty("name").equalsIgnoreCase("System Business Owner"));
  }
}
