/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by Enrico Risa (e.risa-at-orientechnologies.com) on 19/09/14.
 */
@Test(groups = "embedded")
public class LuceneSingleFieldEmbeddedTest extends BaseLuceneTest {

  public LuceneSingleFieldEmbeddedTest() {
    super(false);
  }

  public LuceneSingleFieldEmbeddedTest(boolean remote) {
    super(remote);
  }

  @Test
  public void loadAndTest() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    databaseDocumentTx.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    List<ODocument> docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(
        "select * from Song where [title] LUCENE \"(title:mountain)\""));

    Assert.assertEquals(docs.size(), 4);

    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>("select * from Song where [author] LUCENE \"(author:Fabbio)\""));

    Assert.assertEquals(docs.size(), 87);

    // not WORK BECAUSE IT USES only the first index
    // String query = "select * from Song where [title] LUCENE \"(title:mountain)\"  and [author] LUCENE \"(author:Fabbio)\""
    String query = "select * from Song where [title] LUCENE \"(title:mountain)\"  and author = 'Fabbio'";
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);
  }

  @BeforeClass
  public void init() {
    initDB();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass song = schema.createClass("Song");
    song.setSuperClass(v);
    song.createProperty("title", OType.STRING);
    song.createProperty("author", OType.STRING);

    databaseDocumentTx.command(new OCommandSQL("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE")).execute();
    databaseDocumentTx.command(new OCommandSQL("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE")).execute();

  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }

  protected String getScriptFromStream(InputStream in) {
    String script = "";
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      StringBuilder out = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        out.append(line + "\n");
      }
      script = out.toString();
      reader.close();
    } catch (Exception e) {

    }
    return script;
  }

  @Override
  protected String getDatabaseName() {
    return "singleField";
  }
}
