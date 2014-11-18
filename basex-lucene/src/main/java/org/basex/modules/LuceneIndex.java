package org.basex.modules;

import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.*;
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

  public static int getID(){
    return NumberSessions;
  }
  
  public Data getData(){
	return dbdata;
  }


  /**
   * Evaluates given Lucene Query and returns pre values
   * of found XML Nodes and facetresults.
   * @param data Database Data
   * @param drillDownField Drilldownfield
   * @throws Exception Exception
   */
  public void drilldown(final String dim, final String... drillDownField) throws Exception {
    IOFile indexpath = dbdata.meta.path;
    IndexReader reader = DirectoryReader.open(FSDirectory.open(
        new File(indexpath.toString() + "/" + "LuceneIndex")));
    IndexSearcher searcher = new IndexSearcher(reader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(
        new File(indexpath.toString() + "/" + "LuceneIndex-taxo")));

    try {
      q.add(dim, drillDownField);

      FacetsCollector facetCollector = new FacetsCollector();
      ScoreDoc[] hits = null;

      hits = FacetsCollector.search(searcher, q, 1000000, facetCollector).scoreDocs;
      
      ArrayList<Integer> pres = new ArrayList<>();
      for(int i = 0; i < hits.length; ++i) {
        int docId = hits[i].doc;
        Document d = searcher.doc(docId);
        Number a = d.getField("pre").numericValue();
        pres.add(dbdata.pre((Integer) a));
      }

      resultContainer = pres;

    } finally {
      reader.close();
      taxoReader.close();
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
      IOFile indexpath = data.meta.path;

      FacetsConfig fconfig = new FacetsConfig();
      fconfig.setHierarchical("text", true);
      fconfig.setHierarchical("att", true);

      baseQuery = new QueryParser(Version.LUCENE_4_9, "text", analyzer).parse(query);
      q = new DrillDownQuery(fconfig, baseQuery);

      IndexReader reader = DirectoryReader.open(FSDirectory.open(
          new File(indexpath.toString() + "/" + "LuceneIndex")));
      IndexSearcher searcher = new IndexSearcher(reader);
     
      try {

        TopScoreDocCollector collector = TopScoreDocCollector.create(10, true);

        searcher.search(baseQuery, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;

        ArrayList<Integer> pres1 = new ArrayList<>();
        for(int i = 0; i < hits.length; ++i) {
          int docId = hits[i].doc;
          Document d = searcher.doc(docId);
          Number a = d.getField("pre").numericValue();
          pres1.add(data.pre((Integer) a));
        }

        resultContainer = pres1;

      } finally {
        reader.close();
      }
  }

  /**
   * Get-Method for Lucene result container.
   * @return ArrayList containing Lucene search results.
   */
  public ArrayList<Integer> getResults() {
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
      /*
      for(int pre = 0; pre < size; pre++) {
        if(data.kind(pre) == Data.TEXT) {
          int parentpre = data.parent(pre, Data.TEXT);
          int parentid = data.id(parentpre);

          int pathpre = parentpre;
          ArrayList<String> path = new ArrayList<>();
          ArrayList<String> attpath = new ArrayList<>();
          while(pathpre > 0) {
            System.out.println(pathpre);
            path.add(Token.string(data.name(pathpre, Data.ELEM)));
            if(data.name(pathpre, Data.ATTR) != null){
            	attpath.add(Token.string(data.name(pathpre, Data.ATTR)));
            }	
            pathpre = data.parent(pathpre, Data.ELEM);
          }

          byte[] text = data.text(pre, true);

          Document doc = new Document();

          String[] pathArray = new String[path.size()];
          Collections.reverse(path);
          path.toArray(pathArray);

          doc.add(new IntField("pre", parentid, Field.Store.YES));
          doc.add(new TextField("text", Token.string(text), Field.Store.YES));
          doc.add(new FacetField("text", pathArray));
          writer.addDocument(fconfig.build(taxoWriter, doc));
        }
    }
    */

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
    fconfig.setHierarchical("text", true);
    fconfig.setHierarchical("att", true);

    IndexReader reader = DirectoryReader.open(FSDirectory.open(
        new File(indexpath.toString() + "/" + "LuceneIndex")));
    IndexSearcher searcher = new IndexSearcher(reader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(
        new File(indexpath.toString() + "/" + "LuceneIndex-taxo")));
    FacetsCollector facetsCollector = new FacetsCollector(true);

    FacetsCollector.search(searcher, new MatchAllDocsQuery(), 1, facetsCollector);
    Facets ftext = new FastTaxonomyFacetCounts(
         taxoReader, fconfig, facetsCollector);

    FacetResult results = ftext.getTopChildren(10, "text");
    LabelAndValue[] labelValues = results.labelValues;

    ArrayList<ANode> facets = new ArrayList<>();

    for(int i = 0; i < labelValues.length; i++) {
      FElem elem = new FElem(labelValues[i].label).add(labelValues[i].value.toString());
      subElems(elem, ftext, labelValues[i].label);
      facets.add(elem);
    }

    return facets;
  }

  /**
   * Used for building hierarchical structure of XML nodes.
   * @param elem FElem
   * @param ftext Facets
   * @param path String...
   * @throws IOException I/OException
   */
  public static void subElems(final FElem elem, final Facets ftext, final String... path)
      throws IOException {
    FacetResult results = ftext.getTopChildren(10, "text", path);

    if(results != null) {
      LabelAndValue[] labelValues = results.labelValues;

      for(int i = 0; i < labelValues.length; i++) {
        FElem subElem = new FElem(labelValues[i].label).add(labelValues[i].value.toString());
        int length = path.length;
        String[] newPath = new String[length + 1];
        System.arraycopy(path, 0, newPath, 0, length);
        newPath[length] = labelValues[i].label;
        subElems(subElem, ftext, newPath);
        elem.add(subElem);
      }
    }

  }

}