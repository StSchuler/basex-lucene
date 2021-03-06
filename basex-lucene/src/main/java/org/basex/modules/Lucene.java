package org.basex.modules;

import java.io.*;
import java.math.*;
import java.util.*;

import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Version;
import org.basex.data.*;
import org.basex.io.*;
import org.basex.query.*;
import org.basex.query.value.*;
import org.basex.query.value.item.*;
import org.basex.query.value.node.*;

/**
 * This module evaluates the lucene:search() command.
 *
 * @author Stephan
 *
 */
public class Lucene extends QueryModule{


  /**
   * Creates new Query Instance with next ID.
   * @return ID
   */
  public Int connect() {
    int id = LuceneIndex.getID();
    new LuceneIndex(id);
    return Int.get(id);
  }
  
  /**
   * Creates Lucene Index of the given Database.
   * The main elements denotes on what basis the Documents 
   * are created.
   * @param name Database name
   * @param mainEle Main elements
   * @throws Exception Exception
   */
  public void luceneIndex(final String name, final String mainEle) throws Exception{
	 Data data = queryContext.resources.database(name, null);
	 LuceneIndex.luceneIndexSchema(queryContext.context, data, name, mainEle);
  }

  /**
   * Queries the given input String and returns
   * a collection of all found ANodes.
   * @param id Query id
   * @param query Query
   * @param name String
   * @throws Exception exception
   */
  public Int search(final Int id, final String query, final String name) throws Exception {
     Data data = queryContext.resources.database(name, null);
     LuceneIndex session = LuceneIndex.getInstance(((BigInteger) id.toJava()).intValue());

     session.query(query, data);

     return id;
   }

  /**
   * Queries the given input String and returns
   * a collection of all found ANodes and drills
   * down on given element.
   * @param id Query id
   * @param dim Dimension
   * @param drillDownTerm Drilldownterm
   * @throws Exception exception
   */
  public Int drillDown(final Int id, final String dim,
      final Value drillDownTerm) throws Exception {
    LuceneIndex session = LuceneIndex.getInstance(((BigInteger) id.toJava()).intValue());

    ArrayList<String> drill = new ArrayList<>();
    for(Item item : drillDownTerm) {
      drill.add((String) item.toJava());
    }
    String[] drillDownField = new String[drill.size()];
    drill.toArray(drillDownField);

    session.drilldown(dim, drillDownField);

    return id;
  }

  /**
   * Display Lucene search results of defined Query.
   * @param id Query id
   * @return ANode[] results
   * @throws QueryException Query exception
   * @throws IOException I/O Exception
   */
  public ANode[] result(final Int id) throws QueryException, IOException {
    LuceneIndex session = LuceneIndex.getInstance(((BigInteger) id.toJava()).intValue());
    Data data = session.getData();

    ArrayList<Integer> resultContainer = session.getResults(false);
    int length = resultContainer.size();
    
    ANode[] nodes = new ANode[length];
    
    for(int i = 0; i < length; i++) {
      int pre =  resultContainer.get(i);
      DBNode n = new DBNode(data, data.pre(pre));
      nodes[i] = n;
    }
    return nodes;

  }
  
  /**
   * Display Lucene facet results of defined Query.
   * @param id Query id
   * @return ANode[] result
   * @throws IOException
   */
  public ANode[] facetResult(final Int id) throws IOException {
	LuceneIndex session = LuceneIndex.getInstance(((BigInteger) id.toJava()).intValue());
	
	session.getResults(true);
	ANode[] nodes = session.getFResult();
	
	return nodes; 
  }

  /**
   * Merges Number Segments of the Index to
   * given Number maxNumSegments.
   * @param dbname Database Name
   * @param maxNumSeg Number of segments
   * @throws QueryException Query Exception
   * @throws IOException I/O Exception
   */
  public void optimize(final String dbname, final Int maxNumSeg)
      throws QueryException, IOException {

    int seg = (int) maxNumSeg.toJava();
    if(seg < 5) {
      StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
      Data data = queryContext.resources.database(dbname, null);
      IOFile indexpath = data.meta.path;
      File indexFile = new File(indexpath.toString() + "/" + "LuceneIndex");
      Directory index = FSDirectory.open(indexFile);
      IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
      IndexWriter writer = new IndexWriter(index, config);

      writer.forceMerge(seg);

      writer.close();
    }
  }

  /**
   * Returns facets of given Database.
   * @param name Database name
   * @return facets of given Database
   * @throws QueryException Query exception
   * @throws IOException  I/O exception
   */
  public ANode[] facets(final String name) throws QueryException, IOException {
    Data data = queryContext.resources.database(name, null);

    ArrayList<ANode> results = LuceneIndex.facet(data);
    int size = results.size();
    ANode[] resultNodes =  new ANode[size];

    results.toArray(resultNodes);

    return resultNodes;
  }
}
