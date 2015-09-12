/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.lucene.manager.OLuceneIndexManagerAbstract;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;

public class OLuceneIndexEngine<V> extends OSharedResourceAdaptiveExternal implements OIndexEngine<V> {

  private final String                  indexType;
  protected OLuceneIndexManagerAbstract lucene;
  protected OIndex                      indexManaged;
  private ODocument                     indexMetadata;

  public OLuceneIndexEngine(OLuceneIndexManagerAbstract delegate, String indexType) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(), OGlobalConfiguration.MVRBTREE_TIMEOUT
        .getValueAsInteger(), true);

    this.lucene = delegate;
    this.indexType = indexType;
  }

  public void init() {
    lucene.init();
  }

  public void flush() {
    lucene.flush();
  }

  public void create(OIndexDefinition indexDefinition, String clusterIndexName, OStreamSerializer valueSerializer,
      boolean isAutomatic) {

    lucene.createIndex(indexDefinition, clusterIndexName, valueSerializer, isAutomatic, indexMetadata);

  }

  public void delete() {
    lucene.delete();
  }

  public void deleteWithoutLoad(String indexName) {
    lucene.deleteWithoutLoad(indexName);
  }

  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, OStreamSerializer valueSerializer,
      boolean isAutomatic) {
    lucene.load(indexRid, indexName, indexDefinition, isAutomatic, indexMetadata);
  }

  public boolean contains(Object key) {
    return lucene.contains(key);
  }

  public boolean remove(Object key) {
    return lucene.remove(key);
  }

  public boolean remove(Object key, OIdentifiable value) {
    OIdentifiable rid = value;
    if (value instanceof ODocument) {
      rid = value.getIdentity();
    }
    return lucene.remove(key, rid);
  }

  public ORID getIdentity() {
    return lucene.getIdentity();
  }

  public void clear() {
    lucene.clear();
  }

  public void close() {
    lucene.close();
  }

  public V get(Object key) {
    return (V) lucene.get(key);
  }

  public void put(Object key, V value) {
    lucene.put(key, value);
  }

  public Object getFirstKey() {
    return lucene.getFirstKey();
  }

  public Object getLastKey() {
    return lucene.getLastKey();
  }

  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer<V> transformer) {
    return lucene.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
  }

  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer<V> transformer) {
    return lucene.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder, transformer);
  }

  public void commit() {

  }

  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer<V> transformer) {
    return lucene.iterateEntriesMinor(toKey, isInclusive, ascSortOrder, transformer);
  }

  public OIndexCursor cursor(ValuesTransformer<V> valuesTransformer) {
    return lucene.cursor(valuesTransformer);
  }

  public OIndexCursor descCursor(ValuesTransformer<V> valuesTransformer) {
    return null;
  }

  public OIndexKeyCursor keyCursor() {
    return lucene.keyCursor();
  }

  public long size(ValuesTransformer<V> transformer) {
    return lucene.size(transformer);

  }

  public boolean hasRangeQuerySupport() {
    return lucene.hasRangeQuerySupport();
  }

  public int getVersion() {
    return 1;
  }

  public void setManagedIndex(OIndex index) {
    this.indexManaged = index;
  }

  public void setIndexMetadata(ODocument indexMetadata) {
    this.indexMetadata = indexMetadata;
  }

  public ODocument getIndexMetadata() {
    return indexMetadata;
  }

  public void setRebuilding(boolean rebuilding) {
    lucene.setRebuilding(rebuilding);
  }

  public IndexSearcher searcher() throws IOException {
    return lucene.getSearcher();
  }

  public void setIndexName(String indexName) {
    lucene.setIndexName(indexName);
  }
}
