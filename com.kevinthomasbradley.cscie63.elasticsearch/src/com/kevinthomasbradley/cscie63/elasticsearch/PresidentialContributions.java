package com.kevinthomasbradley.cscie63.elasticsearch;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.json.JSONArray;
import org.json.JSONObject;

import au.com.bytecode.opencsv.CSVReader;
import static org.elasticsearch.node.NodeBuilder.*;

/**
 * @author Kevin Thomas Bradley
 * @dateCreated 4-May-2015
 * @description This is the main class of the project
 * @version 1.0
 * @codeReviewer
 */
public class PresidentialContributions {
	
	private static Node node;
	private static Client client;
	
	private static String clusterName = "cscie63";
	private static String indexName = "presidential";
	private static String typeName = "contribution";
	private static boolean populateData = false;
	private static boolean performTasks = true;
	
	private static Map<String, String> mappedStates = null;
	private static CSVReader statesCSV;
	private static CSVReader contribsCSV;
	private static String statesCSVFilename = "/Users/kevinbradley/Desktop/BradleyKevin_Final/Data/states.csv";
	private static String contribsCSVFilename = "/Users/kevinbradley/Desktop/BradleyKevin_Final/Data/2008_Pres_Contrib_subset.csv"; //2008_Pres_Contrib_subset OR 2008_Pres_Contrib
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String args[]) {
		// Connect to ES
		if (connectES()) {
			if (populateData) {
				// Read States CSV and store in KeyValue pair
				mappedStates = readStates(statesCSVFilename);
				// Process contributions
				processContributions(contribsCSVFilename, mappedStates);
				refreshIndex();
			}
			
			if (performTasks) {
				// Perform simple query
				String searchIdResponse = fieldQuery("_id", "SA17A.5211");
				System.out.println("\nSearching for _id SA17A.5211");
				System.out.println("=============================");
				Contribution contrib_SA17A5211 = getSearchResults(searchIdResponse).get(0);
				System.out.println("Document returned: " + contrib_SA17A5211.toString());
				System.out.println("=============================");
						
				// Perform range query
				String searchRangeResponse = rangeQuery("amount", 100, 200);
				System.out.println("\nSearching for Amount between 100 and 200");
				System.out.println("========================================");
				for(Contribution c : getSearchResults(searchRangeResponse)) {
					System.out.println("Document returned: " + c.toString());
				}
				System.out.println("========================================");
				
				// Perform query regarding total contribution per zip code
				String summedAmount_10003 = summedAmount("amount", "contributorZip", "10003");
				System.out.println("\nSearching for Summed Amount for ZIP 10003");
				System.out.println("========================================");
				System.out.println("Value: $" + summedAmount_10003);
				System.out.println("========================================");
				
				// Update contrib_SA17A5211 value
				Double val = contrib_SA17A5211.getAmount()+1;
				contrib_SA17A5211.setAmount(val);
				updateDocument(contrib_SA17A5211);
				refreshIndex();
				System.out.println("\n=============================");
				System.out.println("Updating Value of SA17A.5211 to $" + val);
				System.out.println("=============================");
				// Perform simple query again on contribution id SA17A5211
				String searchIdResponseAgain = fieldQuery("_id", "SA17A.5211");
				System.out.println("\nSearching for _id SA17A.5211 AGAIN");
				System.out.println("=============================");
				Contribution contrib_SA17A5211_updated = getSearchResults(searchIdResponseAgain).get(0);
				System.out.println("Document returned: " + contrib_SA17A5211_updated.toString());
				System.out.println("=============================");
				
				System.out.println("\n=============================");
				System.out.println("Deleting document SA17A.5211");
				System.out.println("=============================");
				deleteDocument(contrib_SA17A5211);
				refreshIndex();
				// Perform simple query again on contribution id SA17A5211
				String searchIdResponseDeleted = fieldQuery("_id", "SA17A.5211");
				System.out.println("\nSearching for _id SA17A.5211 AGAIN");
				System.out.println("=============================");
				int returnedDocs = getSearchResults(searchIdResponseDeleted).size();
				System.out.println("Document count matching search for Id SA17A.5211: " + returnedDocs);
				System.out.println("============================="); 
			}
		}
		closeES();
	}
	
	/**
	 * This method refreshes the index which is required whilst running the 
	 * app once
	 */
	static void refreshIndex() {
		// After updating a document in-memory node, I need to refresh the index
		client.admin().indices().prepareRefresh().execute().actionGet();
	}
	
	/**
	 * This method is used to connect to Elasticsearch
	 * and setup the client
	 * @return
	 */
	static boolean connectES() {
		try {
			node = nodeBuilder().clusterName(clusterName).client(true).node();
			client = node.client();
		} catch (Exception e) {
			System.out.println("ERROR: " + e.getMessage());
			return false;
		}
		return !node.isClosed();
	}
	
	/**
	 * This method is used to close the connections to the client
	 * and node
	 */
	static void closeES() {
		node.close();
		client.close();
	}
	
	/**
	 * This method is used to map the JSON response onto a list
	 * of Contribution objects
	 * @param response
	 * @return
	 */
	static List<Contribution> getSearchResults(String response) {
		JSONObject obj = new JSONObject(response);
		JSONArray results = obj.getJSONObject("hits").getJSONArray("hits");
		List<Contribution> contribs = new ArrayList<Contribution>();
		ObjectMapper mapper = new ObjectMapper();
		for(int i = 0; i < results.length(); i++ ) {
			String result = results.getJSONObject( i).get("_source").toString();
			try {
				Contribution r = mapper.readValue(result, Contribution.class);
				contribs.add(r);
			} catch (IOException e) {
				System.out.println("ERROR: " + e.getMessage());
				return null;
			}
		}
		return contribs;
	}
	
	/**
	 * This method is used to read the states CSV file
	 * @return
	 */
	static Map<String,String> readStates(String csvFilename) {
		Map<String,String> stat = new HashMap<String, String>();
		statesCSV = readCSV(csvFilename);
		String [] nextLine;
	    try {
	    	int count = 0;
			while ((nextLine = statesCSV.readNext()) != null) {
				if (count > 0) {
					stat.put(nextLine[1], nextLine[0]);
				}
				count++;
			}
	    } catch (Exception e) {
	    	System.out.println("ERROR: " + e.getMessage());
	    }
	    return stat;
	}
	
	/**
	 * This method is used to setup the CSVReader and obtain
	 * the file data
	 * @param filename
	 * @return
	 */
	static CSVReader readCSV(String filename) {
		CSVReader reader = null;
		try {
			reader = new CSVReader(new FileReader(filename));
		} catch (Exception e) {
			System.out.println("ERROR: " + e.getMessage());
		}
		return reader;
	}
	
	/**
	 * This method is used to process each of the contributions
	 * @param csvFilename
	 * @param mappedStates
	 */
	static void processContributions(String csvFilename, Map<String, String> mappedStates) { 
		contribsCSV = readCSV(csvFilename);
		String [] nextLine;
	    try {
	    	int count = 0;
			while ((nextLine = contribsCSV.readNext()) != null) {
				if (count > 0) {
					// Read each line of data csv and store in object
					Contribution contrib = contributionFactory(nextLine, mappedStates);
					// Add document to ES
					if (addDocument(contrib))
						System.out.println("(" + count + ") Document created: " + contrib.getId());
					else
						System.out.println("(" + count + ") Document NOT created: " + contrib.getId());
				}
				count++;
			}
	    } catch (Exception e) {
	    	System.out.println("ERROR: " + e.getMessage());
	    }
	}
	
	/**
	 * This method is used to build a new Contribution object
	 * and map the values of the CSV onto this object
	 * @param row
	 * @param states
	 * @return
	 */
	static Contribution contributionFactory(String[] row, Map<String, String> states) {
		Contribution contrib = new Contribution();
		contrib.setId( row[16] ); 							//tran_id
		contrib.setCandidateName( row[2] ); 				// cand_nm
		contrib.setContributorName( row[3] );				// contbr_nm
		contrib.setContributorCity( row[4] );				// contbr_city
		contrib.setContributorStateCode( row[5] );			// contbr_st
		
		contrib.setContributorState( 
			states.get( contrib.getContributorStateCode() ) // mapped state
		);
		
		contrib.setContributorZip( row[6] );				// contbr_zip
		contrib.setContributorEmployer( row[7] );			// contbr_employer
		contrib.setContributorOccupation( row[8] );			// contbr_occupation
		contrib.setAmount( Double.parseDouble( row[9] ) );	// contbr_receipt_amt
		contrib.setDate( row[10] );							// contbr_receipt_dt
		
		return contrib;
	}
	
	/**
	 * This method is used to convert a Contribution object
	 * into a JSON string
	 * @param contrib
	 * @return
	 */
	static String convertContribution(Contribution contrib) {
		String json = "";
		ObjectMapper mapper = new ObjectMapper();
		try {
			json = mapper.writeValueAsString(contrib);
		} catch (JsonGenerationException e) {
			System.out.println("ERROR: " + e.getMessage());
		} catch (JsonMappingException e) {
			System.out.println("ERROR: " + e.getMessage());
		} catch (IOException e) {
			System.out.println("ERROR: " + e.getMessage());
		}
		return json; 
	}

	/**
	 * This method is used to add a new document to our node
	 * @param contrib
	 * @return
	 */
	public static boolean addDocument(Contribution contrib) {
		IndexResponse response = client.prepareIndex(indexName, typeName, contrib.getId())
		        .setSource(convertContribution(contrib))
		        .execute()
		        .actionGet();
		return response.isCreated();
	}
	
	/**
	 * This method is used to query our index by field and value
	 * @param field
	 * @param queryParam
	 * @return
	 */
	static String fieldQuery(String field, String queryParam) {
		SearchResponse response = client.prepareSearch(indexName)
		        .setTypes(typeName)
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.termQuery(field, queryParam))
		        .setFrom(0)
		        .setSize(10)
		        .setExplain(true)
		        .execute()
		        .actionGet();
		return response.toString();
	}
	
	/**
	 * This method is used to perform a range search on a field
	 * @param field
	 * @param from
	 * @param to
	 * @return
	 */
	static String rangeQuery(String field, int from, int to) {
		SearchResponse response = client.prepareSearch(indexName)
		        .setTypes(typeName)
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setPostFilter(FilterBuilders.rangeFilter(field).from(from).to(to))
		        .setFrom(0)
		        .setSize(10)
		        .setExplain(true)
		        .execute()
		        .actionGet();
		return response.toString();
	}
	
	/**
	 * This method is used to perform an aggregation sum of a field
	 * @param aggField
	 * @param queryField
	 * @param queryParam
	 * @return
	 */
	static String summedAmount(String aggField, String queryField, String queryParam) {
		String aggName = "summedAmountForState";
		SearchResponse response = client.prepareSearch(indexName)
		        .setTypes(typeName)
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),
						   FilterBuilders.termFilter(queryField,queryParam)))
			    .addAggregation(
			        AggregationBuilders.sum(aggName).field(aggField)
			     )
			    .execute()
			    .actionGet();
		return getAggValue(aggName, response.toString());
	}
	
	/**
	 * This method is used to obtain the agg value returned
	 * @param response
	 * @return
	 */
	static String getAggValue(String aggName, String response) {
		JSONObject obj = new JSONObject(response);
		JSONObject aggs = obj.getJSONObject("aggregations");
		if (aggs != null) {
			String result = aggs.getJSONObject(aggName).get("value").toString();
			return result;
		} else {
			return "No value";
		}
	}
	
	/**
	 * This method is used to update an existing document
	 * @param contrib
	 */
	static void updateDocument(Contribution contrib) {
		// To illustrate a different way of performing a transaction
		// this code varies slightly from the client.prepareUpdate()
		// which would follow the search and index methods
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index(indexName);
		updateRequest.type(typeName);
		updateRequest.id(contrib.getId());
		updateRequest.doc(convertContribution(contrib));
		try {
			client.update(updateRequest).get();
		} catch (InterruptedException e) {
			System.out.println("ERROR: " + e.getMessage());
		} catch (ExecutionException e) {
			System.out.println("ERROR: " + e.getMessage());
		}

	}
	
	/**
	 * This method is used to delete an existing document
	 * @param contrib
	 */
	static void deleteDocument(Contribution contrib) {
		@SuppressWarnings("unused")
		DeleteResponse response = client.prepareDelete(indexName, typeName, contrib.getId())
		        .execute()
		        .actionGet();
	}

}
