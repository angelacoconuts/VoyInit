package com.SemanticParser;

import java.io.ByteArrayOutputStream;

import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolutionMap;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;

public class AccessDBpediaRemote {

	private String testEndpointQuery = "ASK { }";
	final static String DBPEDIA_VIRTUOSO = "http://dbpedia.org/sparql";

	public ResultSet execParameterisedSelect(String serviceEndpoint,
			String sparqlQueryString, String[] prefixList,
			String inputVar, String inputVarValue) {

		Query query = createParameterizedQuery(sparqlQueryString, prefixList, inputVar, inputVarValue);

		App.logger.info(query.toString());

		ResultSet results = execSelectQuery(serviceEndpoint, query);

		return results;

	}

	/**
	 * Fire a SPARQL against service endpoint
	 * 
	 * @param serviceEndpoint
	 *            SPARQL service endpoint e.g: http://dbpedia.org/sparql
	 * @param sparqlQueryString
	 *            SPARQL query string
	 * @param prefixList
	 *            List of prefixes in the query
	 * @return com.hp.hpl.jena.query.ResultSet
	 */
	public ResultSet execSimpleSelect(String serviceEndpoint,
			String sparqlQueryString, String[] prefixList) {

		Query query = createSimpleQuery(sparqlQueryString, prefixList);

		ResultSet results = execSelectQuery(serviceEndpoint, query);

		return results;

	}

	private Query createParameterizedQuery(String sparqlQueryString,
			String[] prefixList, String inputVar, String inputVarValue) {

		Model model = ModelFactory.createDefaultModel();

		QuerySolutionMap initialBindings = new QuerySolutionMap();

		RDFNode bindingNode = model.createResource(inputVarValue);
		initialBindings.add(inputVar, bindingNode);

		ParameterizedSparqlString queryStr = new ParameterizedSparqlString(
				sparqlQueryString, initialBindings);

		setPrefixes(queryStr, prefixList);

		Query query = queryStr.asQuery();
		return query;

	}

	private Query createSimpleQuery(String sparqlQueryString,
			String[] prefixList) {

		ParameterizedSparqlString queryStr = new ParameterizedSparqlString(
				sparqlQueryString);

		setPrefixes(queryStr, prefixList);

		Query query = queryStr.asQuery();

		return query;
	}

	private ResultSet execSelectQuery(String serviceEndpoint, Query query) {

		ResultSet results = null;
//		ByteArrayOutputStream logWriter = new ByteArrayOutputStream();
			
		QueryExecution qexec = QueryExecutionFactory.sparqlService(
				serviceEndpoint, query);

		App.logger.info("Executing query: "+query.toString());
		results = qexec.execSelect();

//		ResultSetFormatter.out(logWriter, results, query);
//		App.logger.info(logWriter.toString());

		return results;

	}

	private ParameterizedSparqlString setPrefixes(
			ParameterizedSparqlString queryStr, String[] prefixList) {

		for (int i = 0; i< prefixList.length; i++)
			queryStr.setNsPrefix(prefixList[i], VoyInit.prefixMap.get(prefixList[i]));

		return queryStr;

	}

	/**
	 * Test whether a service endpoint is up and running or not
	 * 
	 * @param serviceEndpoint
	 * @return test result (boolean)
	 */
	public boolean testEndpoint(String serviceEndpoint) {

		Query query = QueryFactory.create(testEndpointQuery);

		QueryExecution qexec = QueryExecutionFactory.sparqlService(
				serviceEndpoint, query);

		try {
			if (qexec.execAsk()) {
				App.logger.info(serviceEndpoint + " is UP!");
				return true;
			}
		} catch (QueryExceptionHTTP e) {
			App.logger.error(serviceEndpoint + " is DOWN!");
			return false;
		}
		return false;

	}
}
