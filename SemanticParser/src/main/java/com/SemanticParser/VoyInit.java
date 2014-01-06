package com.SemanticParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;

import net.sf.json.JSONObject;

public class VoyInit {

	static final String POSTGRES_QUERY_GET_PREFIX = "SELECT * FROM VOYPREFIX";
	static final String POSTGRES_QUERY_GET_QUERIES = "SELECT * FROM VOYQUERY";
	static final String POSTGRES_QUERY_GET_QUERY_DATA = "SELECT * FROM VOYQUERY_DATA";
	static final String POSTGRES_QUERY_UPDATE_QUERY_DATA = "INSERT INTO VOYQUERY_DATA VALUES";
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

	public void run() {

		db = new AccessPostgres();

		// fallbackPostgres();
		// initPostgres();
		// initNeo4j();

		ArrayList<SPARQLQuery> queries = null;
		buildPrefixMapping();
		queries = getSPARQLQueries(" WHERE SEQ=1");
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

		} catch (UniformInterfaceException ex) {

			App.logger.error("Bad Request!", ex);

		} catch (ClientHandlerException ex) {

			App.logger.error("Connection refused, server may be down", ex);

		}

		// Insert continent nodes
		java.sql.ResultSet result = db
				.execSelect(POSTGRES_QUERY_GET_QUERY_DATA);

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

		java.sql.ResultSet result = db.execSelect(POSTGRES_QUERY_GET_PREFIX);

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

		java.sql.ResultSet result = db.execSelect(POSTGRES_QUERY_GET_QUERIES
				+ queriesCondition);

		try {

			parsedQueries = new ArrayList<SPARQLQuery>();

			while (result.next()) {

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
							.execSelect(POSTGRES_QUERY_GET_QUERY_DATA
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

			query.setOutputRelations(((String) result
					.getObject(OUTPUT_REl_COLUMN)).split(VAR_DELIMITOR));

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

			while (SPARQLresult.hasNext()) {

				long startNodeInsertTime = System.currentTimeMillis();

				String uri = SPARQLresult.next()
						.getResource(query.getOutputVariables()[0]).getURI();

				// Parameters : (String uri, String input_var)
				// Insert output into VOYQUERY_DATA to be used as input for next
				// iteration
				insertQueryDataEntry(uri, outputClass);

				// Parameters : (String nodeURIStr, String nodeClass, String
				// parentURIStr, String parentClass)
				insertNewNodeNeo4j(uri, outputClass,
						query.getInputVariableValue(), query.getInputVariable());

				App.logger.info("Inserted new node in milliseconds: "
						+ (System.currentTimeMillis() - startNodeInsertTime));
				
			}
		}

		else {
			// Insert relationship, node into Neo4j
			// Insert attribute from SPARQL query result to Neo4j node
		}
	}

	private void insertQueryDataEntry(String uri, String input_var) {

		// Insert interim result into voyquery_data table
		if (!input_var.equals("")) {

			String insert_str = POSTGRES_QUERY_UPDATE_QUERY_DATA + "('"
					+ uri.replace("'", "''") + "','" + input_var + "');";
			db.execUpdate(insert_str);

		}

	}

	private void insertNewNodeNeo4j(String nodeURIStr, String nodeClass,
			String parentURIStr, String parentClass) {

		JSONObject queryResp = null;
		URI newNode = null;
		AccessNeo4j neo4j = new AccessNeo4j();

		try {

			// Check if node exist in Neo4j or not
			queryResp = neo4j.executeCypherQuery("MATCH (n:poi { uri: '"
					+ nodeURIStr + "' }) RETURN n");

			ArrayList<JSONObject> resultList = (ArrayList<JSONObject>) neo4j
					.getCypherResponseAttribute(queryResp, "self",
							JSONObject.class);

			if (resultList.size() == 0) {

				// Insert new node into Neo4j
				String nodeProperties = new JSONObject()
						.element("uri", nodeURIStr).element("class", nodeClass)
						.toString();

				newNode = neo4j.createNode("poi", nodeProperties);
				App.logger.debug("Add node : " + nodeURIStr);

				// Get parent node according to input variable and input
				// variable uri
				queryResp = neo4j.executeCypherQuery("MATCH (n:poi { uri: '"
						+ parentURIStr + "' }) RETURN n");

				ArrayList<String> resultListStr = (ArrayList<String>) neo4j
						.getCypherResponseAttribute(queryResp, "self",
								String.class);

				if (resultListStr.size() != 0) {

					URI parentNode = URI.create(resultListStr.get(0));
					String relationProperties = new JSONObject().element(
							"parent_class", parentClass).toString();

					neo4j.addRelationship(newNode, parentNode, "BELONGS",
							relationProperties);

					App.logger.debug(String.format(
							"Add relationship from [%s] to [%s]",
							newNode.toString(), parentNode.toString()));
				}
			} else {
				// Add attribute to the existing node
			}

		} catch (UniformInterfaceException ex) {

			App.logger.error("Bad Request!", ex);

		} catch (ClientHandlerException ex) {

			App.logger.error("Connection refused, server may be down", ex);

		}

	}
}
