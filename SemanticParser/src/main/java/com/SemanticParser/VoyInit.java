package com.SemanticParser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
	AccessPostgres db = new AccessPostgres();

	public void run() {

		ArrayList<SPARQLQuery> queries = null;

		buildPrefixMapping();
		queries = getSPARQLQueries(" WHERE SEQ=0");
		executeSPARQLQueriesRemote(queries);

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

			App.logger.error(ex.getMessage());
			App.logger.error("SQL Exception: ", ex);

		} finally {
			db.closeResultSet(result);
		}
	}

	public ArrayList<SPARQLQuery> getSPARQLQueries(String queriesClassCondition) {

		ArrayList<SPARQLQuery> queries = null;
		java.sql.ResultSet inputVarValues = null;
		SPARQLQuery query = null;
		String inputVar = null;
		java.sql.ResultSet result = db.execSelect(POSTGRES_QUERY_GET_QUERIES
				+ queriesClassCondition);

		try {

			queries = new ArrayList<SPARQLQuery>();

			while (result.next()) {

				inputVar = (String) result.getObject(INPUT_VAR_COLUMN);

				if (inputVar.equals("")) {

					query = parseSPARQLQueryParameter(result);

					queries.add(query);

				}

				else {

					inputVarValues = db
							.execSelect(POSTGRES_QUERY_GET_QUERY_DATA
									+ " WHERE " + INPUT_VAR_COLUMN + "='"
									+ inputVar + "'");

					while (inputVarValues.next()) {

						query = parseSPARQLQueryParameter(result);

						query.setInputVariable(inputVar);

						query.setInputVariableValue((String) inputVarValues
								.getObject(URI_COLUMN));

						queries.add(query);

					}

				}

			}

			return queries;

		} catch (SQLException ex) {

			App.logger.error(ex.getMessage());
			App.logger.error("SQL Exception: ", ex);

		} finally {
			db.closeResultSet(result);
			if (!inputVar.equals(""))
				db.closeResultSet(inputVarValues);
		}

		return queries;

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

			App.logger.error(ex.getMessage());
			App.logger.error("SQL Exception: ", ex);

		}

		return query;

	}

	public void executeSPARQLQueriesRemote(ArrayList<SPARQLQuery> queries) {

		AccessDBpediaRemote dbpedia = new AccessDBpediaRemote();
		com.hp.hpl.jena.query.ResultSet SPARQLresult = null;

		for (SPARQLQuery query : queries) {

			if (query.getInputVariable() == null)
				SPARQLresult = dbpedia.execSimpleSelect(
						AccessDBpediaRemote.DBPEDIA_VIRTUOSO,
						query.getQueryStr(), query.getPrefixes());
			else{
				SPARQLresult = dbpedia
						.execParameterisedSelect(
								AccessDBpediaRemote.DBPEDIA_VIRTUOSO,
								query.getQueryStr(), query.getPrefixes(),
								query.getInputVariable(),
								query.getInputVariableValue());
			}

			parseSPARQLResult(SPARQLresult, query);
		}

	}

	public void parseSPARQLResult(com.hp.hpl.jena.query.ResultSet SPARQLresult, SPARQLQuery query){

		String uri = null;
		String[] outputs = query.getOutputVariables();
		AccessPostgres db = new AccessPostgres();
		
		if(query.getIsLeaveQuery() == 0) {
			
			//Insert interim result into voyquery_data table
			if(!outputs[0].equals("")){
							
				while(SPARQLresult.hasNext()){
					uri = SPARQLresult.next().getResource(outputs[0]).getURI().replace("'", "''");
					App.logger.info(POSTGRES_QUERY_UPDATE_QUERY_DATA+"('"+uri+"','"+outputs[0]+"');");
					db.execUpdate(POSTGRES_QUERY_UPDATE_QUERY_DATA+"('"+uri+"','"+outputs[0]+"');");
				}

			}
			
			//Insert relationship, node into Neo4j
		}		
		else {
			//Insert relationship, node into Neo4j
			//Insert attribute into Neo4j
		}
	}
}
