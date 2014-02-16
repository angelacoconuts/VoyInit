package com.SemanticParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import net.sf.json.JSONObject;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;

public class VoyInit {

	static final String POSTGRES_GET_PREFIX = "SELECT * FROM VOYPREFIX ";
	static final String POSTGRES_GET_QUERIES = "SELECT * FROM VOYQUERY ";
	static final String POSTGRES_GET_QUERY_DATA = "SELECT * FROM VOYQUERY_DATA ";
	static final String POSTGRES_UPDATE_QUERY_DATA = "INSERT INTO VOYQUERY_DATA VALUES";
	
	static final String PREFIX_COLUMN = "PREFIX";
	static final String URI_COLUMN = "URI";
	static final String SEQ_COLUMN = "SEQ";
	static final String QUERY_COLUMN = "QUERY";
	static final String IS_LEAVE_QUERY_COLUMN = "IS_LEAVE_QUERY";
	static final String INPUT_VAR_COLUMN = "INPUT_VAR";
	static final String OUTPUT_VAR_COLUMN = "OUTPUT_VAR";
	static final String OUTPUT_REl_COLUMN = "OUTPUT_REL";
	static final String VAR_DELIMITOR = ";";

	static Map<String, String> prefixMap = null;
	AccessPostgres db = null;

	public VoyInit() {
		db = new AccessPostgres();
	}

	public void run(String querySeqCondition) {

		ArrayList<SPARQLQuery> queries = null;
		buildPrefixMapping();
		queries = getSPARQLQueries(querySeqCondition);
		executeSPARQLQueriesRemote(queries);

	}

	public boolean initPostgres() {

		String createTblFile = "/home/angelacoconuts/Documents/dev/git/VoyInit/create_table.sql";
		String initTblFile = "/home/angelacoconuts/Documents/dev/git/VoyInit/init_table.sql";

		try {

			db.execScript(new BufferedReader(new FileReader(createTblFile)));
			db.execScript(new BufferedReader(new FileReader(initTblFile)));

			return true;

		} catch (FileNotFoundException ex) {

			App.logger.error("FileNotFoundException: ", ex);

		}

		return false;

	}

	public boolean fallbackPostgres() {

		String fallbackTblFile = "/home/angelacoconuts/Documents/dev/git/VoyInit/fallback_table.sql";

		try {

			db.execScript(new BufferedReader(new FileReader(fallbackTblFile)));

			return true;

		} catch (FileNotFoundException ex) {

			App.logger.error("FileNotFoundException: ", ex);

		}

		return false;

	}

	public void initNeo4j() {

		AccessNeo4j neo4j = new AccessNeo4j();

		try {
			// Create index
			neo4j.createIndex("poi", "uri");
			neo4j.createIndex("poi", "label_en");

		} catch (UniformInterfaceException ex) {

			App.logger.error("Bad Request!", ex);

		} catch (ClientHandlerException ex) {

			App.logger.error("Connection refused, server may be down", ex);

		}

		// Insert continent nodes
		java.sql.ResultSet result = db
				.execSelect(POSTGRES_GET_QUERY_DATA);

		try {

			while (result.next()) {

				String uri = (String) result.getObject(URI_COLUMN);

				String properties = new JSONObject().element("uri", uri)
						.element("class", "continent").toString();

				neo4j.createNode("poi", properties);

			}

		} catch (SQLException ex) {

			App.logger.error(ex.getMessage());
			App.logger.error("SQL Exception: ", ex);

		} catch (UniformInterfaceException ex) {

			App.logger.error("Bad Request!", ex);

		} catch (ClientHandlerException ex) {

			App.logger.error("Connection refused, server may be down", ex);

		} finally {

			db.closeResultSet(result);

		}
	}

	public void buildPrefixMapping() {

		prefixMap = new HashMap<String, String>();

		java.sql.ResultSet result = db.execSelect(POSTGRES_GET_PREFIX);

		try {

			while (result.next()) {

				String prefix = (String) result.getObject(PREFIX_COLUMN);

				prefixMap.put(prefix, (String) result.getObject(URI_COLUMN));
				App.logger.debug("Add prefix " + prefix + ":"
						+ prefixMap.get(prefix));

			}

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		} finally {

			db.closeResultSet(result);

		}
	}

	public ArrayList<SPARQLQuery> getSPARQLQueries(String queriesCondition) {

		ArrayList<SPARQLQuery> parsedQueries = null;
		java.sql.ResultSet inputVarValues = null;
		SPARQLQuery query = null;
		String inputVar = null;

		java.sql.ResultSet result = db.execSelect(POSTGRES_GET_QUERIES
				+ queriesCondition);

		try {

			parsedQueries = new ArrayList<SPARQLQuery>();

			while (result.next()) {

				App.logger.info("Executing query : "
						+ (String) result.getObject(QUERY_COLUMN));

				inputVar = (String) result.getObject(INPUT_VAR_COLUMN);

				if (inputVar.equals("")) {

					// No input variable required, directly parse parameter and
					// call DBpedia
					query = parseSPARQLQueryParameter(result);

					parsedQueries.add(query);

				}

				else {

					// Input variable required, select from VOYQUERY_DATA and
					// iterate through variables
					inputVarValues = db
							.execSelect(POSTGRES_GET_QUERY_DATA
									+ " WHERE " + INPUT_VAR_COLUMN + "='"
									+ inputVar + "'");

					while (inputVarValues.next()) {

						query = parseSPARQLQueryParameter(result);

						query.setInputVariable(inputVar);

						query.setInputVariableValue((String) inputVarValues
								.getObject(URI_COLUMN));

						parsedQueries.add(query);

					}

				}

			}

			return parsedQueries;

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		} finally {

			db.closeResultSet(result);
			if (!inputVar.equals(""))
				db.closeResultSet(inputVarValues);

		}

		return parsedQueries;

	}

	private SPARQLQuery parseSPARQLQueryParameter(java.sql.ResultSet result) {

		SPARQLQuery query = null;

		try {

			query = new SPARQLQuery();

			query.setQueryStr((String) result.getObject(QUERY_COLUMN));

			query.setPrefixes(((String) result.getObject(PREFIX_COLUMN))
					.split(VAR_DELIMITOR));

			query.setIsLeaveQuery((Integer) result
					.getObject(IS_LEAVE_QUERY_COLUMN));

			query.setOutputVariables(((String) result
					.getObject(OUTPUT_VAR_COLUMN)).split(VAR_DELIMITOR));

			query.setOutputRelation((String) result
					.getObject(OUTPUT_REl_COLUMN));

			return query;

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		}

		return query;

	}

	public void executeSPARQLQueriesRemote(ArrayList<SPARQLQuery> queries) {

		AccessDBpediaRemote dbpedia = new AccessDBpediaRemote();
		com.hp.hpl.jena.query.ResultSet SPARQLresult = null;

		for (SPARQLQuery query : queries) {

			long startSPARQLQueryTime = System.currentTimeMillis();

			if (query.getInputVariable() == null)
				SPARQLresult = dbpedia.execSimpleSelect(
						AccessDBpediaRemote.DBPEDIA_VIRTUOSO,
						query.getQueryStr(), query.getPrefixes());
			else {
				SPARQLresult = dbpedia
						.execParameterisedSelect(
								AccessDBpediaRemote.DBPEDIA_VIRTUOSO,
								query.getQueryStr(), query.getPrefixes(),
								query.getInputVariable(),
								query.getInputVariableValue());
			}

			App.logger.info("Queried dbpedia remote in milliseconds: "
					+ (System.currentTimeMillis() - startSPARQLQueryTime));

			parseSPARQLResult(SPARQLresult, query);

		}

	}

	public void parseSPARQLResult(com.hp.hpl.jena.query.ResultSet SPARQLresult,
			SPARQLQuery query) {

		String outputClass = query.getOutputVariables()[0];

		if (query.getIsLeaveQuery() == 0) {

			List<String> resultList = getResultSetColumnURI(SPARQLresult,
					outputClass);

			for (String uri : resultList) {

				long startNodeInsertTime = System.currentTimeMillis();

				// Parameters : (String uri, String input_var)
				// Insert output into VOYQUERY_DATA to be used as input for next
				// iteration
				insertQueryDataEntry(uri, outputClass);

				// Parameters : (String outputNodeURIStr,
				// String outputNodeClass, String inputNodeURIStr,
				// String inputNodeClass, String outputRelation)
				insertNewNodeNeo4j(uri, outputClass,
						query.getInputVariableValue(),
						query.getInputVariable(), query.getOutputRelation());

				App.logger.info("Processed node in milliseconds: "
						+ (System.currentTimeMillis() - startNodeInsertTime));

			}
		}

		else {

			long startNodeTime = System.currentTimeMillis();

			addLeaveNodeAttributes(SPARQLresult, query);

			App.logger.info("Add node attributes in milliseconds: "
					+ (System.currentTimeMillis() - startNodeTime));

		}
	}

	private void addLeaveNodeAttributes(
			com.hp.hpl.jena.query.ResultSet SPARQLresult, SPARQLQuery query) {

		// Leave node
		// Insert attribute from SPARQL query result to Neo4j node
		AccessNeo4j neo4j = new AccessNeo4j();
		String[] outputVarList = query.getOutputVariables();
		URI leaveNode = getNodeLocationByURIStr(query.getInputVariableValue());

		App.logger.info("Add attributes to : " + query.getInputVariableValue());

		try {
			if (outputVarList.length != 1) {

				// A list of attributes in one row
				while (SPARQLresult.hasNext()) {

					QuerySolution result = SPARQLresult.next();
					// A list of attributes in one row
					for (int i = 0; i < outputVarList.length; i++) {

						RDFNode attributeNode = result.get(outputVarList[i]);

						if (attributeNode != null) {

							// long startNeo4jTime = System.currentTimeMillis();

							neo4j.addProperty(leaveNode, outputVarList[i],
									attributeNode.toString());

							// App.logger.info("Neo4j access time: "+
							// (System.currentTimeMillis() - startNeo4jTime));

						}

					}
				}

			} else {

				List<String> outputValueList = new ArrayList<String>();

				// Multiple rows containing one attributes e.g: list of links
				while (SPARQLresult.hasNext()) {
					outputValueList.add("\\\""
							+ SPARQLresult.next().get(outputVarList[0])
									.toString() + "\\\"");
				}

				neo4j.addProperty(leaveNode, outputVarList[0],
						outputValueList.toString());

			}

		} catch (UniformInterfaceException ex) {

			App.logger.error("Bad Request!", ex);

		} catch (ClientHandlerException ex) {

			App.logger.error("Connection refused, server may be down", ex);

		}
	}

	private List<String> getResultSetColumnURI(ResultSet queryResult,
			String columnName) {

		List<String> resultURIList = new ArrayList<String>();

		try {

			while (queryResult.hasNext()) {
				String uri = queryResult.next().getResource(columnName)
						.getURI();
				resultURIList.add(uri);
			}

		} catch (Exception ex) {

			App.logger.error("Error getting resource from SPARQL query", ex);

		}

		return resultURIList;

	}

	private void insertQueryDataEntry(String uri, String input_var) {

		// Insert interim result into voyquery_data table
		if (!input_var.equals("")) {

			String insert_str = POSTGRES_UPDATE_QUERY_DATA + "('"
					+ uri.replace("'", "''") + "','" + input_var + "');";
			db.execUpdate(insert_str);

		}

	}

	private void insertNewNodeNeo4j(String outputNodeURIStr,
			String outputNodeClass, String inputNodeURIStr,
			String inputNodeClass, String outputRelation) {

		AccessNeo4j neo4j = new AccessNeo4j();

		try {

			URI outputNode = getNodeLocationByURIStr(outputNodeURIStr);

			// Check if node exist in Neo4j or not

			if (outputNode == null) {

				// Insert new node into Neo4j
				String nodeProperties = new JSONObject()
						.element("uri", outputNodeURIStr)
						.element("class", outputNodeClass).toString();

				outputNode = neo4j.createNode("poi", nodeProperties);
				App.logger.info("Add node : " + outputNodeURIStr);

			}

			// Get parent node according to input variable and input variable
			// uri

			URI inputNode = getNodeLocationByURIStr(inputNodeURIStr);

			if (inputNode != null) {

				if (outputRelation.toUpperCase().equals("BELONGS")) {

					String relationProperties = new JSONObject().element(
							"parent_class", inputNodeClass).toString();

					neo4j.addRelationship(outputNode, inputNode, "BELONGS",
							relationProperties);

					// App.logger.info(String.format("Add BELONGS relationship from [%s] to [%s]",
					// outputNode.toString(), inputNode.toString()));

				}

				else if (outputRelation.toUpperCase().equals("CONTAINS")) {

					String relationProperties = new JSONObject().element(
							"parent_class", outputNodeClass).toString();

					neo4j.addRelationship(inputNode, outputNode, "BELONGS",
							relationProperties);

					// App.logger.info(String.format("Add BELONGS relationship from [%s] to [%s]",
					// inputNode.toString(), outputNode.toString()));

				}
			}

		} catch (UniformInterfaceException ex) {

			App.logger.error("Bad Request!", ex);

		} catch (ClientHandlerException ex) {

			App.logger.error("Connection refused, server may be down", ex);

		}

	}

	private URI getNodeLocationByURIStr(String URIString) {

		AccessNeo4j neo4j = new AccessNeo4j();

		try {

			JSONObject queryResp = neo4j.executeCypherQuery(
					"MATCH (n:poi { uri: \"" + URIString + "\" }) RETURN n",
					"{}");

			ArrayList<String> nodeLocation = (ArrayList<String>) neo4j
					.getCypherResponseAttribute(queryResp, "self", String.class);

			if (nodeLocation.size() != 0) {
				// Node found
				URI node = URI.create(nodeLocation.get(0));
				return node;

			}

		} catch (UniformInterfaceException ex) {
			App.logger.error("Bad Request!", ex);
		} catch (ClientHandlerException ex) {
			App.logger.error("Connection refused, server may be down", ex);
		}

		return null;

	}
	
	public void loadGEONamesCountries(){
		
		AccessNeo4j neo4j = new AccessNeo4j();
				
		java.sql.ResultSet result = db
				.execSelect("SELECT COUNTRY_NAME FROM VOYCOUNTRIES;");

		try {

			while (result.next()) {

				String countryName = (String) result.getObject("COUNTRY_NAME");

				String mergeNodeQuery = String.format("{ \"query\":\"MERGE (n:[%s] { uri:[%s], class:[%s] }) RETURN n\" }"
						,"poi"
						,"'http://dbpedia.org/resource/"+StringUtils.replace(countryName, " ","_") + "'"
						,"'country'");
				
				URI node = neo4j.mergeNode(mergeNodeQuery);
				
				App.logger.info(node.toString());

			}
			
		}catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		}catch (UniformInterfaceException ex) {

			App.logger.error("Bad Request!", ex);
			
		}
		
	}
	
	public void loadGEONamesCities(){
		
		AccessNeo4j neo4j = new AccessNeo4j();
				
		java.sql.ResultSet result = db
				.execSelect("SELECT CITY_NAME, COUNTRY_NAME"
						+ " FROM VOYCITIES a, VOYCOUNTRIES b"
						+ " WHERE a.COUNTRY_CD = b.COUNTRY_CD;");

		try {

			while (result.next()) {

				String countryURI = "'http://dbpedia.org/resource/"+StringUtils.replace((String) result.getObject("COUNTRY_NAME"), " ","_") + "'";
				String cityURI = "'http://dbpedia.org/resource/"+StringUtils.replace(StringUtils.replace((String) result.getObject("CITY_NAME"), " ","_"),"'","\\\\'") + "'";

				String mergeNodeQuery = String.format("{ \"query\":\" MATCH (p:poi { uri:%s, class:'country' })"
						+ " MERGE (c:poi { uri:%s })"
						+ " -[:BELONGS]-> (p)"
						+ " ON CREATE SET c.class='city'"
						+ " ON MATCH SET c.class='city'"
						+ " RETURN c\" }"
						,countryURI
						,cityURI);
				
				URI node = neo4j.mergeNode(mergeNodeQuery);
				
				App.logger.info(node.toString());

			}
			
		}catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		}catch (UniformInterfaceException ex) {

			App.logger.error("Bad Request!", ex);
			
		}
		
	}
}
