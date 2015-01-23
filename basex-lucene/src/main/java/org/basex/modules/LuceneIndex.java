package org.basex.modules;

import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.facet.taxonomy.*;
import org.apache.lucene.facet.taxonomy.directory.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.*;
import org.apache.lucene.queryparser.classic.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Version;
import org.basex.core.*;
import org.basex.data.*;
import org.basex.io.*;
import org.basex.query.value.node.*;
import org.basex.util.Token;
import org.basex.util.list.IntList;
import org.basex.util.list.TokenList;

/**
 * Builds Lucene Index for current database context and provides
 * search functions.
 *
 * @author Stephan
 *
 */
public final class LuceneIndex {
  /**
   * Lucene Analyzer.
   */
  private static StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
  /**
   * Lucene Query.
   */
  private Query baseQuery;
  /**
   * Lucene drill-down Query;
   */
  private DrillDownQuery q;
  /**
   * ArrayList containing Lucene search results.
   */
  private ArrayList<Integer> resultContainer = new ArrayList<>();
  /**
   * HashMap containing all query Instances.
   */
  private static HashMap<Integer, LuceneIndex> querysessions = new HashMap<>();
  /**
   * Total number of Query Sessions.
   */
  private static int NumberSessions = 1;
  /**
   * Data of current DB.
   */
  private Data dbdata;
  /**
   * Lucene DrillSidewasResult for current query.
   */
  private DrillSidewaysResult dsResult;
  /**
   * Lucene facet result nodes.
   */
  private ANode[] nodes;

  /**
   * Constructor.
   * @param key Integer
   */
  public LuceneIndex(Integer key) {
    NumberSessions++;
    querysessions.put(key, this);
  }

  /**
   * Get Query Session.
   * @param id Session ID
   * @return LuceneIndex
   */
  public static LuceneIndex getInstance(Integer id) {
    return querysessions.get(id);
  }

  /**
   * Get next session ID.
   * @return ID
   */
  public static int getID(){
    return NumberSessions;
  }
  
  /**
   * Get Database data of this session.
   * @return Database data
   */
  public Data getData(){
	return dbdata;
  }
  
  /**
   * Get facet result of this session.
   * @return Facet result 
   */
  public ANode[] getFResult(){
	return nodes;
  }


  /**
   * Evaluates given Lucene Query and returns pre values
   * of found XML Nodes and facetresults.
   * @param dim Dimension
   * @param drillDownField Drilldownfield
   * @throws Exception Exception
   */
  public void drilldown(final String dim, final String... drillDownField) throws Exception {
	 if(drillDownField.length > 1){
	   q.add(dim, drillDownField);
	 }
	 else{
	   q.add(dim, new QueryParser(Version.LUCENE_4_9, "text", analyzer).parse(drillDownField[0]));
	 }
  }


  /**
   * Evaluates given Lucene Query and returns pre values
   * of found XML Nodes.
   * @param query Query
   * @param data Data
   * @throws ParseException Parse exception
   * @throws IOException I/O exception
   */
  public void query(final String query, final Data data)
      throws ParseException, IOException {

	  dbdata = data;

      FacetsConfig fconfig = new FacetsConfig();

      baseQuery = new QueryParser(Version.LUCENE_4_9, "text", analyzer).parse(query);
      q = new DrillDownQuery(fconfig, baseQuery);
  }

  /**
   * Get-Method for Lucene results of defined query.
   * @param flag Facetflag
   * @return ArrayList containing Lucene search results.
   * @throws IOException I/O exception
   */
  public ArrayList<Integer> getResults(final Boolean flag) throws IOException {
	IOFile indexpath = dbdata.meta.path;
	IndexReader reader = DirectoryReader.open(FSDirectory.open(
	  new File(indexpath.toString() + "/" + "LuceneIndex")));
	IndexSearcher searcher = new IndexSearcher(reader);
	TaxonomyReader taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(
	  new File(indexpath.toString() + "/" + "LuceneIndex-taxo")));  
	 
	try{
	  FacetsCollector facetCollector = new FacetsCollector();
	  ScoreDoc[] hits = null;

	  hits = FacetsCollector.search(searcher, q, 1000000, facetCollector).scoreDocs;
	  
	  FacetsConfig fconfig = new FacetsConfig();
	  
	  DrillSideways ds = new DrillSideways(searcher, fconfig, taxoReader);
	  dsResult = ds.search(q, 10);
	  
	  ArrayList<Integer> pres = new ArrayList<>();
	    for(int i = 0; i < hits.length; ++i) {
	      int docId = hits[i].doc;
	      Document d = searcher.doc(docId);
	      Number a = d.getField("pre").numericValue();
	      pres.add(dbdata.pre((Integer) a));
	    }
	      
	  resultContainer = pres;
	  
	  if(flag == true) {
	    Facets ftext = dsResult.facets;
	    ArrayList<ANode> fnodes = new ArrayList<>();
	    
	    List<FacetResult> fResult = ftext.getAllDims(Integer.MAX_VALUE);
		int flength = fResult.size();
		
	    for(int i = 0; i < flength; i++) {
	    fnodes = LuceneIndex.elems(fResult, ftext);
	    }
		
	    nodes = new ANode[fnodes.size()];
	    fnodes.toArray(nodes);
	  }

	} finally {
	    reader.close();
	    taxoReader.close();
	}
	  
    return resultContainer;
  }

  /**
   * Builds luceneIndex of current database context.
   * @param context database context
   * @throws Exception exception
   */
  public static void luceneIndex(final Context context) throws Exception {
    IOFile indexpath = context.globalopts.dbpath();
    String dbname = context.data().meta.name;

    File indexFile = new File(indexpath.toString() + "/" + dbname + "/" + "LuceneIndex");
    System.out.println(indexpath.toString() + "/" + dbname + "/" + "LuceneIndex");
    File taxoIndexFile = new File(indexpath.toString() + "/" + dbname + "/" + "LuceneIndex-taxo");
    
    indexFile.mkdir();

    FacetsConfig fconfig = new FacetsConfig();
    fconfig.setHierarchical("text", true);
    fconfig.setHierarchical("att", true);

    Directory index = FSDirectory.open(indexFile);
    Directory taxoIndex = FSDirectory.open(taxoIndexFile);
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
    IndexWriter writer = new IndexWriter(index, config);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoIndex, OpenMode.CREATE);

    try {
      Data data = context.data();
      int size = data.meta.size;

      IntList pres = new IntList();
      TokenList names = new TokenList();
      TokenList att = new TokenList();
      System.out.println(size);
      
      for(int pre = 0; pre < size; pre++) {
        int kind = data.kind(pre);
        int par = data.parent(pre, kind);

        while(!pres.isEmpty() && pres.peek() >= par) {
          pres.pop();
          names.pop();
          if(!att.isEmpty()){
        	  att.pop();
          }
        }
        
        if(kind == Data.ELEM) {
          pres.push(par);
          names.add(data.name(pre, kind));
        } else if(kind == Data.ATTR) {
          att.add(data.name(pre, Data.ATTR));
        } else if(kind == Data.TEXT) {
          int parid = data.id(par);
          final byte[] text = data.text(pre, true);
        
          Document doc = new Document();
          
          doc.add(new IntField("pre", parid, Field.Store.YES));
          doc.add(new TextField("text", Token.string(text), Field.Store.YES));
          doc.add(new FacetField("text", names.toStringArray()));
          if(att.size() > 0) {
            doc.add(new FacetField("att", att.toStringArray()));
          }
        
          writer.addDocument(fconfig.build(taxoWriter, doc));
        }
      }

      writer.forceMerge(5);
      writer.commit();
      taxoWriter.commit();

    } finally {
      writer.close();
      taxoWriter.close();
    }
  }

  /**
   * Builds luceneIndex of current database context with specific schema.
   * @param context database context
   * @param data database data 
   * @param dbname database name
   * @param mainElem main element
   * @throws Exception exception
   */
  public static void luceneIndexSchema(final Context context,final Data data,final String dbname, final String mainEle) throws Exception {
	    IOFile indexpath = context.globalopts.dbpath();
	  
	    File indexFile = new File(indexpath.toString() + "/" + dbname + "/" + "LuceneIndex");
	    File taxoIndexFile = new File(indexpath.toString() + "/" + dbname + "/" + "LuceneIndex-taxo");
	    
	    indexFile.delete();
	    taxoIndexFile.delete();
	    indexFile.mkdir();
	    taxoIndexFile.mkdir();

	    FacetsConfig fconfig = new FacetsConfig();

	    Directory index = FSDirectory.open(indexFile);
	    Directory taxoIndex = FSDirectory.open(taxoIndexFile);
	    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
	    IndexWriter writer = new IndexWriter(index, config);
	    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoIndex, OpenMode.CREATE);

	    try {
	      int size = data.meta.size;
	      System.out.println("size:" + size);
	      
	      for(int pre = 0; pre < size; pre++) {
	        //int kind = data.kind(pre);
	        //int par = data.parent(pre, kind);
	        
	        
	        if(data.kind(pre) == Data.ELEM){
	       
	        if(Token.string(data.name(pre, Data.ELEM)).equals(mainEle)) {
	          System.out.println(true);
	          int tsize = data.size(pre, Data.ELEM);
	          Document doc = new Document();
	          doc.add(new IntField("pre", data.id(pre), Field.Store.YES));
	          
	          
	          for(int i = pre+1; i < pre+tsize; i++){
	        	if(pre < size){
	        	int csize = data.size(i, Data.ELEM);
	        	System.out.println("csize:" + csize);
	        	int ckind = data.kind(i);
		        int cpar = data.parent(i, ckind);  
	        	
		  
	        	if(data.kind(i) == Data.ELEM && csize > 2){
	        	  ArrayList<String> patharray = new ArrayList<>();
	        	  System.out.println("deep" + Token.string(data.name(i, Data.ELEM)));
	        		
	        	  for(int j = i; j < i+csize; j++){
	        		int cckind = data.kind(j);
			        //int ccpar = data.parent(j, ckind);  
	        		
			        if(cckind == Data.TEXT){
			          final byte[] text = data.text(j, true);
			          patharray.add(Token.string(text));
	        		
			          doc.add(new TextField("text", Token.string(text), Field.Store.YES));
			        }
	        	  }
	        	  String[] path = new String[patharray.size()];
	        	  patharray.toArray(path);
	        	  
	        	  fconfig.setHierarchical(Token.string(data.name(i, Data.ELEM)), true);
	        	  doc.add(new FacetField(Token.string(data.name(i, Data.ELEM)), path));
	        	  i += csize;
	        	}
	        	else{
	              if(ckind == Data.TEXT){
	                final byte[] text = data.text(i, true);
	                doc.add(new TextField("text", Token.string(text), Field.Store.YES));
	                fconfig.setMultiValued(Token.string(data.name(cpar, Data.ELEM)), true);
	                System.out.println(Token.string(text));
	                doc.add(new FacetField(Token.string(data.name(cpar, Data.ELEM)), Token.string(text)));
	              }
	        	}
	        	}
	        	//pre += i;
	          }  
	          writer.addDocument(fconfig.build(taxoWriter, doc));
	          System.out.println(doc);
	        }
	        }
	        
	      }

	      writer.forceMerge(5);
	      writer.commit();
	      taxoWriter.commit();

	    } finally {
	      writer.close();
	      taxoWriter.close();
	    }
	  }

  /**
   * Queries all facets of given Database and builds
   * XML structure for facet results.
   * @param data Data
   * @return ArrayList of ANode
   * @throws IOException I/O exception
   */
  public static ArrayList<ANode> facet(final Data data) throws IOException {
    IOFile indexpath = data.meta.path;

    FacetsConfig fconfig = new FacetsConfig();

    IndexReader reader = DirectoryReader.open(FSDirectory.open(
        new File(indexpath.toString() + "/" + "LuceneIndex")));
    IndexSearcher searcher = new IndexSearcher(reader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(
        new File(indexpath.toString() + "/" + "LuceneIndex-taxo")));
    FacetsCollector facetsCollector = new FacetsCollector(true);

    FacetsCollector.search(searcher, new MatchAllDocsQuery(), 1, facetsCollector);
    Facets ftext = new FastTaxonomyFacetCounts(
         taxoReader, fconfig, facetsCollector);

    List<FacetResult> results = ftext.getAllDims(Integer.MAX_VALUE);
    
    ArrayList<ANode> facets = new ArrayList<>();
    facets = elems(results, ftext);
    
    reader.close();
    taxoReader.close();

    return facets;
  }
  
  /**
   * Used to build XML elements for facets.
   * @param results facet results
   * @param ftext facets
   * @return ANodes containing facet information
   * @throws IOException
   */
  public static ArrayList<ANode> elems(final List<FacetResult> results, final Facets ftext) throws IOException {
	int flength = results.size();
	   
	ArrayList<ANode> facets = new ArrayList<>();
	FElem facetElem = new FElem("facets");
	    
	for(int i = 1; i < flength+1; i++) {
	  FacetResult fresult = results.get(i-1);
	  String fdim = fresult.dim;
	  LabelAndValue[] value = fresult.labelValues;
      int laVLength = value.length;
		    
	  FElem mainElem = new FElem("category");
	  mainElem.add("name", fdim);
	  facetElem.add(mainElem);
	  for(int j = 0; j < laVLength; j++){
		FElem elem = new FElem("entry").add("number", value[j].value.toString());
		FElem valueElem = new FElem("value").add(value[j].label.toString());
		elem.add(valueElem);
		subElems(elem, ftext, fdim, value[j].label);
		mainElem.add(elem);
	  }
	}
	facets.add(facetElem);
	
	return facets;
  }

  /**
   * Used for building hierarchical structure of XML nodes.
   * @param elem FElem
   * @param ftext Facets
   * @param path String...
   * @throws IOException I/OException
   */
  public static void subElems(final FElem elem, final Facets ftext, final String dim, final String... path)
      throws IOException {
    FacetResult results = ftext.getTopChildren(10, dim, path);

    if(results != null) {
      LabelAndValue[] labelValues = results.labelValues;

      for(int i = 0; i < labelValues.length; i++) {
        FElem subElem = new FElem("entry").add("number", labelValues[i].value.toString());
        FElem valueElem = new FElem("value").add(labelValues[i].label.toString());
        subElem.add(valueElem);
        int length = path.length;
        String[] newPath = new String[length + 1];
        System.arraycopy(path, 0, newPath, 0, length);
        newPath[length] = labelValues[i].label;
        subElems(subElem, ftext, dim, newPath);
        elem.add(subElem);
      }
    }

  }

}