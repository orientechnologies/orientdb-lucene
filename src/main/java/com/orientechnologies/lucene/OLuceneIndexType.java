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

import com.orientechnologies.lucene.collections.OFullTextCompositeKey;
import com.orientechnologies.lucene.manager.OLuceneIndexManagerAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Version;

import java.util.*;

/**
 * Created by enricorisa on 21/03/14.
 */
public class OLuceneIndexType {

    public static Field createField(String fieldName, OIdentifiable oIdentifiable, Object value, Field.Store store,
                                    Field.Index analyzed) {
        Field field = null;

        if (value instanceof Number) {
            Number number = (Number) value;
            if (value instanceof Long) {
                field = new LongField(fieldName, number.longValue(), store);
            } else if (value instanceof Float) {
                field = new FloatField(fieldName, number.floatValue(), store);
            } else if (value instanceof Double) {
                field = new DoubleField(fieldName, number.doubleValue(), store);
            } else {
                field = new IntField(fieldName, number.intValue(), store);
            }
        } else if (value instanceof Date) {
            field = new LongField(fieldName, ((Date) value).getTime(), store);

        } else {
            field = new Field(fieldName, value.toString(), store, analyzed);

        }
        return field;
    }

    public static Query createExactQuery(OIndexDefinition index, Object key) {

        Query query = null;
        if (key instanceof String) {
            BooleanQuery booleanQ = new BooleanQuery();
            if (index.getFields().size() > 0) {
                for (String idx : index.getFields()) {
                    booleanQ.add(new TermQuery(new Term(idx, key.toString())), BooleanClause.Occur.SHOULD);
                }
            } else {
                booleanQ.add(new TermQuery(new Term(OLuceneIndexManagerAbstract.KEY, key.toString())), BooleanClause.Occur.SHOULD);
            }
            query = booleanQ;
        } else if (key instanceof OCompositeKey) {
            BooleanQuery booleanQ = new BooleanQuery();
            int i = 0;
            OCompositeKey keys = (OCompositeKey) key;
            for (String idx : index.getFields()) {
                String val = (String) keys.getKeys().get(i);
                booleanQ.add(new TermQuery(new Term(idx, val)), BooleanClause.Occur.MUST);
                i++;

            }
            query = booleanQ;
        }
        return query;
    }

    public static Query createQueryId(OIdentifiable value) {
        return new TermQuery(new Term(OLuceneIndexManagerAbstract.RID, value.toString()));
    }

    public static Query createDeleteQuery(OIdentifiable value, List<String> fields, Object key) {

        BooleanQuery booleanQuery = new BooleanQuery();

        booleanQuery.add(new TermQuery(new Term(OLuceneIndexManagerAbstract.RID, value.toString())), BooleanClause.Occur.MUST);

        Map<String, String> values = new HashMap<String, String>();
        //TODO Implementation of Composite keys with Collection
        if (key instanceof OCompositeKey) {


        } else {
            values.put(fields.iterator().next(), key.toString());
        }
        for (String s : values.keySet()) {
            booleanQuery.add(new TermQuery(new Term(s + OLuceneIndexManagerAbstract.STORED, values.get(s))), BooleanClause.Occur.MUST);
        }
        return booleanQuery;
    }

    public static Query createFullTextQuery(OIndexDefinition index, Object key, Analyzer analyzer, Version version) throws ParseException {

        String query;
        Boolean multi = null;

        if (key instanceof OFullTextCompositeKey) {
            query = ((OFullTextCompositeKey) key).getParameters().get("query").toString();
            Object parsertype = ((OFullTextCompositeKey) key).getParameters().get("parsertype");
            if (parsertype != null) {
                if (parsertype.toString().equalsIgnoreCase("MultiField")) { multi = true; }
                if (parsertype.toString().equalsIgnoreCase("Class")) { multi = false; }
            }
        } else {
            query = key.toString();
        }

        if (multi == null) {
            multi = !(query.startsWith("(") & query.endsWith(")"));
        }

        return getQueryParser(index, key, multi, analyzer, version).parse(query);
    }

    protected static QueryParser getQueryParser(OIndexDefinition index, Object key, Boolean multi, Analyzer analyzer, Version version)
            throws ParseException {
        Map options;
        QueryParser queryParser;

        if (key instanceof OFullTextCompositeKey) {
            options = ((OFullTextCompositeKey) key).getParameters();
        } else {
            options = new HashMap<String, Object>();
        }

        if (options.containsKey("parsertype")) {
            String pt = options.get("parsertype").toString();
            if (pt.equalsIgnoreCase("MultiField")) { multi = true; }
            if (pt.equalsIgnoreCase("Class")) { multi = false; }
        }

        if (multi) {
            queryParser = new MultiFieldQueryParser(version, index.getFields().toArray(new String[index.getFields().size()]), analyzer);
        } else {
            queryParser = new QueryParser(version, "", analyzer);
        }

        if (options.containsKey("lowercaseexpandedterms")) {
            String let = options.get("lowercaseexpandedterms").toString();
            queryParser.setLowercaseExpandedTerms(Boolean.parseBoolean(let));
        }

        if (options.containsKey("allowleadingwildcard")) {
            String alw = options.get("allowleadingwildcard").toString();
            queryParser.setAllowLeadingWildcard(Boolean.parseBoolean(alw));
        }

        if (options.containsKey("analyzerangeterms")) {
            String art = options.get("analyzerangeterms").toString();
            queryParser.setAnalyzeRangeTerms(Boolean.parseBoolean(art));
        }

        if (options.containsKey("autogeneratephrasequeries")) {
            String agpq = options.get("autogeneratephrasequeries").toString();
            queryParser.setAllowLeadingWildcard(Boolean.parseBoolean(agpq));
        }

        if (options.containsKey("dateresolution")) {
            String dr = options.get("dateresolution").toString();
            queryParser.setDateResolution(DateTools.Resolution.valueOf(dr));
        }

        if (options.containsKey("defaultoperator")) {
            String dop = options.get("defaultoperator").toString();
            queryParser.setDefaultOperator(QueryParser.Operator.valueOf(dop));
        }

        if (options.containsKey("fuzzyminsim")) {
            String fms = options.get("fuzzyminsim").toString();
            queryParser.setFuzzyMinSim(Float.parseFloat(fms));
        }

        if (options.containsKey("fuzzyprefixlength")) {
            String fpl = options.get("fuzzyprefixlength").toString();
            queryParser.setFuzzyPrefixLength(Integer.parseInt(fpl));
        }

        if (options.containsKey("locale")) {
            String l = options.get("locale").toString();
            queryParser.setLocale(new Locale(l));
        }

        if (options.containsKey("phraseslop")) {
            String pl = options.get("phraseslop").toString();
            queryParser.setPhraseSlop(Integer.parseInt(pl));
        }

        if (options.containsKey("timezone")) {
            String tl = options.get("timezone").toString();
            queryParser.setTimeZone(TimeZone.getTimeZone(tl));
        }

        if (options.containsKey("multitermrewritemethod")) {
            String mtrm = options.get("multitermrewritemethod").toString();

            if (mtrm.equalsIgnoreCase("SCORING_BOOLEAN_QUERY_REWRITE")) {
                queryParser.setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
            } else if (mtrm.equalsIgnoreCase("CONSTANT_SCORE_AUTO_REWRITE_DEFAULT")) {
                queryParser.setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT);
            } else if (mtrm.equalsIgnoreCase("CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE")) {
                queryParser.setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
            } else if (mtrm.equalsIgnoreCase("CONSTANT_SCORE_FILTER_REWRITE")) {
                queryParser.setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
            }

        }

        return queryParser;
    }

    public static Sort sort(Query query, OIndexDefinition index, boolean ascSortOrder) {
        String key = index.getFields().iterator().next();
        Number number = ((NumericRangeQuery) query).getMin();
        number = number != null ? number : ((NumericRangeQuery) query).getMax();
        SortField.Type fieldType = SortField.Type.INT;
        if (number instanceof Long) {
            fieldType = SortField.Type.LONG;
        } else if (number instanceof Float) {
            fieldType = SortField.Type.FLOAT;
        } else if (number instanceof Double) {
            fieldType = SortField.Type.DOUBLE;
        }

        return new Sort(new SortField(key, fieldType, ascSortOrder));
    }

}
