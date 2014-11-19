package com.juma.mohammad;

import java.io.File;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class SimpleDBSearcher {
    
	private static final String LUCENE_QUERY = "SellanApp";
	private static final int MAX_HITS = 100;

	public static void main(String[] args) throws Exception {
        File indexDir = new File(SimpleDBIndexer.INDEX_DIR);
        String query = LUCENE_QUERY;
        SimpleDBSearcher searcher = new SimpleDBSearcher();
        searcher.searchIndex(indexDir, query);
    }
    
    private void searchIndex(File indexDir, String queryStr) throws Exception {
        System.out.println(indexDir);
        System.out.println(queryStr);
        Directory directory = FSDirectory.open(indexDir);
        QueryParser queryParser = new QueryParser("cfp_name", new StandardAnalyzer());
        //MultiFieldQueryParser queryParser = new MultiFieldQueryParser(new String[] {"cfp_id","cfp_name"}, new StandardAnalyzer());        
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        queryParser.setPhraseSlop(0);
        queryParser.setLowercaseExpandedTerms(true);
        Query query = queryParser.parse(queryStr);
        
        TopDocs topDocs = searcher.search(query, MAX_HITS);
        ScoreDoc[] hits = topDocs.scoreDocs;
        System.out.println(topDocs);
        System.out.println(query);
        System.out.println(hits.length + " Record(s) Found");
        for (int i = 0; i < hits.length; i++) {
            int docId = hits[i].doc;
            System.out.println(docId);
            Document d = searcher.doc(docId);
            
            String url = d.get("path"); // get its path field
			System.out.println("Found in :: " + url);
			
            System.out.println("ID: " +d.get("cfp_id") + ", Name: " + d.get("cfp_name"));
        }
        if(hits.length ==0){
        	System.out.println("No Data Found");
        }
    }

}

