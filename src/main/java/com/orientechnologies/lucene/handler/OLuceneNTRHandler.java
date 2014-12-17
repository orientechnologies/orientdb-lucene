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

package com.orientechnologies.lucene.handler;

import com.orientechnologies.common.log.OLogManager;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;

import java.io.IOException;

/**
 * Created by Enrico Risa on 17/12/14.
 */
public class OLuceneNTRHandler extends OLuceneHandler {

  protected SearcherManager                searcherManager;
  protected TrackingIndexWriter            mgrWriter;
  protected ControlledRealTimeReopenThread nrt;
  private long                             reopenToken;

  public OLuceneNTRHandler(IndexWriter writer) {
    super(writer);
  }

  @Override
  public void addDocument(Document doc) throws IOException {
    reopenToken = mgrWriter.addDocument(doc);
  }

  @Override
  public void deleteDocument(Query query) throws IOException {
    reopenToken = mgrWriter.deleteDocuments(query);
  }

  @Override
  public IndexWriter getIndexWriter() throws IOException {
    return mgrWriter.getIndexWriter();
  }

  @Override
  public IndexReader getIndexReader() throws IOException {
    return getSearcher().getIndexReader();
  }

  @Override
  public IndexSearcher getIndexSearcher() throws IOException {
    return getSearcher();
  }

  @Override
  protected void initHandler(IndexWriter indexWriter) {

    try {
      mgrWriter = new TrackingIndexWriter(indexWriter);
      searcherManager = new SearcherManager(indexWriter, true, null);
      if (nrt != null) {
        nrt.close();
      }

      nrt = new ControlledRealTimeReopenThread(mgrWriter, searcherManager, 60.00, 0.1);
      nrt.setDaemon(true);
      nrt.start();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on initializing Lucene NTR index", e);
    }
  }

  @Override
  public void closeIndex() throws IOException {
    nrt.interrupt();
    nrt.close();
    searcherManager.close();
    mgrWriter.getIndexWriter().commit();
    mgrWriter.getIndexWriter().close();
  }

  protected IndexSearcher getSearcher() throws IOException {
    try {
      nrt.waitForGeneration(reopenToken);
    } catch (InterruptedException e) {
      OLogManager.instance().error(this, "Error on get searcher from Lucene index", e);
    }
    return searcherManager.acquire();
  }
}
