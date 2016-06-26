package elasticsearchClient;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.QueryBuilders.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.highlight.HighlightField;
import org.json.JSONObject;


public class NodeClient {

	private Node node;
	private Client client;

	private FileManager fileManager;

	private String index_name = "notices";
	private String type_name = "notice";
	
	/***********************/
	public static void main(String[] args) {

		
//		NodeClient nodeClient = new NodeClient();
		
//		nodeClient.deleteIndex(index_name);
//		
//		nodeClient.createIndex(index_name);
//		
//		nodeClient.index(index_name, type_name);

//		nodeClient.shutdown();
	}

	/***********************/
	public NodeClient() {

		// on startup
		node = nodeBuilder().node();
		client = node.client();

		fileManager = new FileManager();
	}	

	/***********************/
	public void deleteIndex() {
		
		DeleteIndexResponse deleteIndexResponse = client.admin().indices()
				.delete(new DeleteIndexRequest(index_name))
				.actionGet();
		
		System.out.println("Delete index " + index_name + " is acknowledged : " + deleteIndexResponse.isAcknowledged());
	}
	
	/***********************/
	public void createIndex() {
		
		CreateIndexResponse createIndexResponse = client.admin().indices()
				.create(new CreateIndexRequest(index_name))
				.actionGet();	
		
		System.out.println("Create index " + index_name + " is acknowledged is : " + createIndexResponse.isAcknowledged());
	}
	
	/***********************/
	public void index() {

		PrintWriter pw = null;
		try {
			// Exception logging
			pw = new PrintWriter(new File("C:/Users/Monireh/Documents/Imaginatio-project/exceptions"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		ArrayList <JSONObject> notices = fileManager.parseJson();
		
		String noticeJson = null;
	
		for (int i = 0; i < notices.size(); i++) {
		
			JSONObject notice = notices.get(i);
			noticeJson = notice.toString(); // notice.toString(4)
			
			
			// Put mapping
			XContentBuilder mapping = null;
			try {		
				 mapping = jsonBuilder()
		                     .startObject()
		                          .startObject(type_name)
		                               .startObject("properties")
		                                   .startObject("WORK.RESOURCE_LEGAL_ID_CELEX.VALUE")
		                                       .field("type", "string")
		                                       .field("index", "not_analyzed")
		                                    .endObject()
		                                    .startObject("source")        // it does what ???
		                                       .field("type","string")    // it does what ???
		                                    .endObject()
		                               .endObject()
		                           .endObject()
		                        .endObject();
				 
				 PutMappingResponse putMappingResponse = client.admin().indices()
					       .preparePutMapping(index_name)
					       .setType(type_name)
					       .setSource(mapping)
					       .execute().actionGet();
							
				System.out.println("Put mapping is acknowledged : " + putMappingResponse.isAcknowledged());
			} 
			catch (ElasticsearchException e) {
				System.out.println("Exception in putting customized map");
				e.printStackTrace();
			} 
			catch (IOException e) {
				System.out.println("IO Exception");
				e.printStackTrace();
			}

			
			//Index a document
		try {
			IndexResponse response = client.prepareIndex(index_name, type_name)
			    .setSource(noticeJson)
			    .execute()
			    .actionGet(); 
			
			System.out.println("Indexing NOTICE " + i + " : "+ response.isCreated());
		 } 
		catch(MapperParsingException mappingParsingException) {
			mappingParsingException.printStackTrace(pw);
		 }
		}

	}
		
	/***********************/
	public void searchByMatch(ArrayList<String> include_matches, ArrayList<String> exclude_matches) {
		
		String fieldName = "WORK.WORK_HAS_EXPRESSION.EMBEDDED_NOTICE.EXPRESSION.EXPRESSION_TITLE.VALUE";
		
		StringBuilder include_terms = new StringBuilder("");
		StringBuilder exclude_terms = new StringBuilder("");
		
		if (!include_matches.isEmpty()) {
			for (String include_match : include_matches) {
				
				include_terms.append(include_match);
				include_terms.append(" ");
			}
		}
		if (!exclude_matches.isEmpty()) {
			for (String exclude_match : exclude_matches) {
				
				exclude_terms.append(exclude_match);
				exclude_terms.append(" ");
			}	
		}
		
		MatchQueryBuilder matchQueryBuilder1 = matchQuery(fieldName, include_terms);
		MatchQueryBuilder matchQueryBuilder2 = matchQuery(fieldName, exclude_terms);
		
		QueryBuilder boolQueryBuilder = boolQuery()
			    .must(matchQueryBuilder1)      
			    .mustNot(matchQueryBuilder2); 
		
		search_query(boolQueryBuilder);
	}

	/***********************/
	public void searchByMatch_Phrase(ArrayList<String> include_match_phrases, ArrayList<String> exclude_match_phrases) {

		String fieldName = "WORK.WORK_HAS_EXPRESSION.EMBEDDED_NOTICE.EXPRESSION.EXPRESSION_TITLE.VALUE";
		
		String include_match_phrase = "";
		String exclude_match_phrase = "";
		
		if (!include_match_phrases.isEmpty()) {
			for (String match_phrase : include_match_phrases) {
				include_match_phrase = match_phrase;
			}
		}
		if (!exclude_match_phrases.isEmpty()) {
			for (String match_phrase : exclude_match_phrases) {
				exclude_match_phrase = match_phrase;
			}
		}
		
		MatchQueryBuilder matchQueryBuilder1 = matchPhraseQuery(fieldName, include_match_phrase);
		MatchQueryBuilder matchQueryBuilder2 = matchPhraseQuery(fieldName, exclude_match_phrase);
		
		QueryBuilder boolQueryBuilder = boolQuery()
			    .must(matchQueryBuilder1) 
			    .mustNot(matchQueryBuilder2); 
		
		search_query(boolQueryBuilder);
	}
	
	/***********************/
	public void searchFullText(ArrayList<String> include_matches, ArrayList<String> exclude_matches,
			ArrayList<String> include_match_phrases, ArrayList<String> exclude_match_phrases) {
		
		String fieldName = "WORK.WORK_HAS_EXPRESSION.EMBEDDED_NOTICE.EXPRESSION.EXPRESSION_TITLE.VALUE";
		
		// matches
		StringBuilder include_terms = new StringBuilder("");
		StringBuilder exclude_terms = new StringBuilder("");
		
		if (!include_matches.isEmpty()) {
			for (String include_match : include_matches) {
				
				include_terms.append(include_match);
				include_terms.append(" ");
			}
		}
		if (!exclude_matches.isEmpty()) {
			for (String exclude_match : exclude_matches) {
				
				exclude_terms.append(exclude_match);
				exclude_terms.append(" ");
			}	
		}
		
		MatchQueryBuilder matchQueryBuilder1 = matchQuery(fieldName, include_terms);
		MatchQueryBuilder matchQueryBuilder2 = matchQuery(fieldName, exclude_terms);
		
		// match_phrases
		String include_match_phrase = "";
		String exclude_match_phrase = "";
		
		if (!include_match_phrases.isEmpty()) {
			for (String match_phrase : include_match_phrases) {
				include_match_phrase = match_phrase;
			}
		}
		if (!exclude_match_phrases.isEmpty()) {
			for (String match_phrase : exclude_match_phrases) {
				exclude_match_phrase = match_phrase;
			}
		}
		
		MatchQueryBuilder matchQueryBuilder3 = matchPhraseQuery(fieldName, include_match_phrase);
		MatchQueryBuilder matchQueryBuilder4 = matchPhraseQuery(fieldName, exclude_match_phrase);
		
		QueryBuilder boolQueryBuilder = boolQuery()
			    .must(matchQueryBuilder1) 
			    .must(matchQueryBuilder3)
			    .mustNot(matchQueryBuilder2)
			    .mustNot(matchQueryBuilder4); 
		
		search_query(boolQueryBuilder);
	}
		
	/***********************/
	public void searchByPublication_Date(String date) {
		
		String fieldName = "WORK.WORK_DATE_DOCUMENT.VALUE";
		
		TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(fieldName, date);
		
//		search_query(termQueryBuilder);
		
		SearchResponse response = client.prepareSearch(index_name)
		        .setTypes(type_name)
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(termQueryBuilder)  
		        .addHighlightedField("WORK.WORK_HAS_EXPRESSION.EMBEDDED_NOTICE.EXPRESSION.EXPRESSION_TITLE.VALUE", 0, 0)
		        .addHighlightedField(fieldName, 0, 0)
		        .setFrom(0)
		        .setSize(10)
		        .setExplain(true)
		        .execute()
		        .actionGet();
		
		
		System.out.println("Search results :");
		System.out.println("Total hits : " + response.getHits().getTotalHits());
		System.out.println("Max score : " + response.getHits().getMaxScore());
		System.out.println("Highlights :");
		
		SearchHit[] hits = response.getHits().getHits();
		for (int i = 0; i < hits.length; i++) {
			
			SearchHit searchHit = hits[i];
			Map<String, HighlightField> field_map = searchHit.getHighlightFields();
			
			for (Map.Entry<String, HighlightField> entry : field_map.entrySet()) {
				
				Text[] fragments = entry.getValue().getFragments();
				System.out.println(fragments[0]);
			}
		}
	}
	
	/***********************/
	public void searchByIdentifier(String identifier) {
		
		String fieldName = "WORK.RESOURCE_LEGAL_ID_CELEX.VALUE";
		
		TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(fieldName, identifier);
		
//		search_query(termQueryBuilder);
		
		SearchResponse response = client.prepareSearch(index_name)
		        .setTypes(type_name)
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(termQueryBuilder)  
		        .addHighlightedField("WORK.WORK_HAS_EXPRESSION.EMBEDDED_NOTICE.EXPRESSION.EXPRESSION_TITLE.VALUE", 0, 0)
		        .addHighlightedField(fieldName, 0, 0)
		        .setFrom(0)
		        .setSize(10)
		        .setExplain(true)
		        .execute()
		        .actionGet();
		
		
		System.out.println("Search results :");
		System.out.println("Total hits : " + response.getHits().getTotalHits());
		System.out.println("Max score : " + response.getHits().getMaxScore());
		System.out.println("Highlights :");
		
		SearchHit[] hits = response.getHits().getHits();
		for (int i = 0; i < hits.length; i++) {
			
			SearchHit searchHit = hits[i];
			Map<String, HighlightField> field_map = searchHit.getHighlightFields();
			
			for (Map.Entry<String, HighlightField> entry : field_map.entrySet()) {
				
				Text[] fragments = entry.getValue().getFragments();
				System.out.println(fragments[0]);
			}
		}
	}
	
	/***********************/
	public void search_query(QueryBuilder queryBuilder) {
		
		SearchResponse response = client.prepareSearch(index_name)
		        .setTypes(type_name)
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(queryBuilder)  
		        .addHighlightedField("WORK.WORK_HAS_EXPRESSION.EMBEDDED_NOTICE.EXPRESSION.EXPRESSION_TITLE.VALUE", 0, 0)
		        .setFrom(0)
		        .setSize(20)
		        .setExplain(true)
		        .execute()
		        .actionGet();
		
		
		System.out.println("Search results :");
		System.out.println("Total hits : " + response.getHits().getTotalHits());
		System.out.println("Max score : " + response.getHits().getMaxScore());
		System.out.println("Highlights :");
		
		
		SearchHit[] hits = response.getHits().getHits();
		for (int i = 0; i < hits.length; i++) {
			
			SearchHit searchHit = hits[i];
			Map<String, HighlightField> field_map = searchHit.getHighlightFields();

			for (Map.Entry<String, HighlightField> entry : field_map.entrySet()) {
				
				Text[] fragments = entry.getValue().getFragments();
				System.out.println(i+1 + " : " + fragments[0]);
			}
		}
	}
	
	/***********************/
	public void shutdown() {

		node.close();
	}
}
