package com.juma.mohammad;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/*
import java.sql.SQLException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
*/

public class SimpleDBIndexer {
	public static final String INDEX_DIR = "D:/Health Care/LuceneTestFolder";
	private static final String JDBC_DRIVER = "oracle.jdbc.OracleDriver";
	private static final String CONNECTION_URL = "jdbc:oracle:thin:@localhost:1522:ORCL";
	private static final String USER_NAME = "C##APPCAT";
	private static final String PASSWORD = "appcat";
	
	private static final String QUERY = "select * from lookup_cfp";
	//private static final String UPDATE_QUERY = "update lookup_cfp set cfp_name = 'SoldOurself' where cfp_name = 'SellanApp' ";
	
	public static void main(String[] args) throws Exception {
		File indexDir = new File(INDEX_DIR);
		SimpleDBIndexer indexer = new SimpleDBIndexer();
		try{  
			   Class.forName(JDBC_DRIVER).newInstance();  
			   Connection conn = DriverManager.getConnection(CONNECTION_URL, USER_NAME, PASSWORD);  
			   Analyzer analyzer = new StandardAnalyzer();  
			   IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_4_10_1, analyzer);
			   IndexWriter indexWriter = new IndexWriter(FSDirectory.open(indexDir), indexWriterConfig);
			   System.out.println("Indexing to directory '" + indexDir + "'...");  
			   int indexedDocumentCount = indexer.indexDocs(indexWriter, conn);  
			   System.out.println(indexedDocumentCount + " records have been indexed successfully");
			   indexWriter.close(); 
				   
			} catch (Exception e) {  
			   e.printStackTrace();  
			} 		
	}
	
	int indexDocs(IndexWriter writer, Connection conn) throws Exception {  
		  String sql = QUERY;  
		  Statement stmt = conn.createStatement();  
		  ResultSet rs = stmt.executeQuery(sql);  
		  int i=0;
		  while (rs.next()) {  
		     Document d = new Document();  
		     d.add(new StringField("cfp_id", rs.getString("cfp_id"), Field.Store.YES));  
		     //System.out.println(rs.getString("cfp_id"));
		     d.add(new StringField("cfp_name", rs.getString("cfp_name"), Field.Store.YES));
		     //System.out.println(rs.getString("cfp_name"));
		     writer.addDocument(d);
		     i++;
		 }  
		  return i;
		}
	
	static void UpdateIndex(IndexWriter writer) throws IOException
	{
	     Document d = new Document();  
	     d.add(new StringField("cfp_id", "11", Field.Store.YES));
	     d.add(new StringField("cfp_name", "SellanApp", Field.Store.YES));
	     //writer.addDocument(d);
	     writer.updateDocument(new Term("SellanApp", INDEX_DIR),d);
	}
	
	/*
			   //Term term = new Term("cfp_name","SellanApp");
			 // System.out.println(term.hashCode());
			   //Document doc = new Document();
			  // doc.add(new Field("cfp_name", "SellanApp11", Field.Store.YES, Field.Index.ANALYZED));
			  // d.add(new Field("cfp_name", rs.getString("cfp_name"), Field.Store.YES, Field.Index.ANALYZED));
			  // indexWriter.updateDocument(term, doc, analyzer);
			   
			   //System.out.println("1");
			   
			   //System.out.println("2");
			   //Term term = new Term("SellanApp");
			   //System.out.println("3");
			   //Document doc = new Document();
			   //System.out.println("4");
			   //indexWriter.updateDocument(term, doc);
			   //System.out.println("3");			   
			   //indexWriter.close(); 
			   
			    Directory directory = FSDirectory.open(indexDir);
				IndexReader indexReader = DirectoryReader.open(directory);
				//IndexReader indexReader = IndexReader.open(directory);
				//indexReader.deleteDocuments(new Term("cfp_name","SellanApp")); 
				indexReader.close();

			   
			   //IndexReader a = IndexReader.open(directory);
			   int numTotalHits = 0;
			   IndexSearcher searcher; 
			   TopDocs results = searcher.search(QUERY, numTotalHits); 
			   ScoreDoc[] hits = results.scoreDocs; 
			   for (int i = 0; i < numTotalHits; i++) 
			   { 
			   doc = searcher.doc(hits[i].doc); 
			   System.out.println( hits[i].doc + " : " + hits[i].score); 
			   }
			   
			   //trying new things
			   //indexer.updateDocs(indexWriter, conn);
			   
			   //Directory directoryReader = new Directory();
			   //IndexSearcher searcher = new IndexSearcher(directoryReader);
			   //TermQuery query = new TermQuery(new Term("field", "term"));
			   //TopDocs topdocs = searcher.query(query, numberToReturn);
			   //int docId = topdocs.scoredocs[i].doc;
				
				//delete indexes for a file
			   	   indexWriter.deleteDocuments(new Term("cfp_name","SellanApp")); 
			   	   //indexWriter.

				   indexWriter.commit();
				   System.out.println("index contains deleted files: "+indexWriter.hasDeletions());
				   System.out.println("index contains documents: "+indexWriter.maxDoc());
				   System.out.println("index contains deleted documents: "+indexWriter.numDocs());
				   
				   indexer.updateDocs(indexWriter, conn);
				   
	
	private void updateDocs(IndexWriter indexWriter, Connection conn) {
		  String sql = UPDATE_QUERY;  
		  Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}
	*/	
}

/*
package com.juma.mohammad;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;


public class SimpleDBIndexer {
	public static final String INDEX_DIR = "D:/Health Care/LuceneTestFolder";
	private static final String JDBC_DRIVER = "oracle.jdbc.OracleDriver";
	private static final String CONNECTION_URL = "jdbc:oracle:thin:@localhost:1522:ORCL";
	private static final String USER_NAME = "C##APPCAT";
	private static final String PASSWORD = "appcat";
	
	private static final String QUERY = "select * from lookup_cfp";
	
	public static void main(String[] args) throws Exception {
		File indexDir = new File(INDEX_DIR);
		SimpleDBIndexer indexer = new SimpleDBIndexer();
		try{  
			   Class.forName(JDBC_DRIVER).newInstance();  
			   Connection conn = DriverManager.getConnection(CONNECTION_URL, USER_NAME, PASSWORD);  
			   SimpleAnalyzer analyzer = new SimpleAnalyzer(Version.LUCENE_35);  
			   IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_35, analyzer);
			   IndexWriter indexWriter = new IndexWriter(FSDirectory.open(indexDir), indexWriterConfig);
			   System.out.println("Indexing to directory '" + indexDir + "'...");  
			   int indexedDocumentCount = indexer.indexDocs(indexWriter, conn);  
			   indexWriter.close();  
			   System.out.println(indexedDocumentCount + " records have been indexed successfully");
			   
			} catch (Exception e) {  
			   e.printStackTrace();  
			} 		
	}

	int indexDocs(IndexWriter writer, Connection conn) throws Exception {  
		  String sql = QUERY;  
		  Statement stmt = conn.createStatement();  
		  ResultSet rs = stmt.executeQuery(sql);  
		  int i=0;
		  while (rs.next()) {  
		     Document d = new Document();  
		     d.add(new Field("cfp_id", rs.getString("cfp_id"), Field.Store.YES, Field.Index.ANALYZED));  
		     d.add(new Field("cfp_name", rs.getString("cfp_name"), Field.Store.YES, Field.Index.ANALYZED));  
		     //d.add(new Field("JOB", rs.getString("JOB"),Field.Store.YES, Field.Index.ANALYZED));
		     //d.add(new Field("MGR", rs.getString("MGR")==null?"":rs.getString("MGR"),Field.Store.YES, Field.Index.ANALYZED));
		     //String hireDateString = DateTools.dateToString(rs.getDate("HIREDATE"), Resolution.DAY);
		     //d.add(new Field("HIREDATE", hireDateString,Field.Store.YES, Field.Index.NOT_ANALYZED));		   
		     //d.add(new Field("SAL", rs.getString("SAL"),Field.Store.YES, Field.Index.ANALYZED));
		     //d.add(new Field("COMM", rs.getString("COMM")==null?"":rs.getString("COMM"),Field.Store.YES, Field.Index.ANALYZED));
		     //d.add(new Field("DEPTNO", rs.getString("DEPTNO"),Field.Store.YES, Field.Index.ANALYZED));
		     //d.add(new Field("DNAME", rs.getString("DNAME"),Field.Store.YES, Field.Index.ANALYZED));
		     //d.add(new Field("LOC", rs.getString("LOC"),Field.Store.YES, Field.Index.ANALYZED));
		     writer.addDocument(d);
		     i++;
		 }  
		  return i;
		}	
}

 */