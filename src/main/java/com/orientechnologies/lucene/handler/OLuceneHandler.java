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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.io.IOException;

/**
 * Created by Enrico Risa on 17/12/14.
 */
public abstract class OLuceneHandler {

  public OLuceneHandler(IndexWriter writer) {
    initHandler(writer);
  }

  public abstract void addDocument(Document doc) throws IOException;

  public abstract void deleteDocument(Query query) throws IOException;

  public abstract IndexWriter getIndexWriter() throws IOException;

  public abstract IndexReader getIndexReader() throws IOException;

  public abstract IndexSearcher getIndexSearcher() throws  IOException;
    
  protected abstract void initHandler(IndexWriter writer);

  public abstract void closeIndex() throws IOException;
}
