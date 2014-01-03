package com.SemanticParser;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

public class AccessDBpedia {

	private String testEndpointQuery = "ASK { }";

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
			String sparqlQueryString, List<String> prefixList) {

		Query query = createSimpleQuery(sparqlQueryString, prefixList);

		ResultSet results = execSelectQuery(serviceEndpoint, query);

		return results;

	}

	public Query createParameterizedQuery(String sparqlQueryString,
			List<String> prefixList, Map<String, String> initialBindingSet) {

		Model model = ModelFactory.createDefaultModel();

		QuerySolutionMap initialBindings = new QuerySolutionMap();

		for (Map.Entry<String, String> bindingEntry : initialBindingSet
				.entrySet()) {

			RDFNode bindingNode = model.createResource(bindingEntry.getValue());
			initialBindings.add(bindingEntry.getKey(), bindingNode);

		}

		ParameterizedSparqlString queryStr = new ParameterizedSparqlString(
				sparqlQueryString, initialBindings);

		setPrefixes(queryStr, prefixList);

		Query query = queryStr.asQuery();
		return query;

	}

	public Query createSimpleQuery(String sparqlQueryString,
			List<String> prefixList) {

		ParameterizedSparqlString queryStr = new ParameterizedSparqlString(
				sparqlQueryString);

		setPrefixes(queryStr, prefixList);

		Query query = queryStr.asQuery();

		return query;
	}

	public ResultSet execSelectQuery(String serviceEndpoint, Query query) {

		ResultSet results = null;
		ByteArrayOutputStream logWriter = new ByteArrayOutputStream();

		QueryExecution qexec = QueryExecutionFactory.sparqlService(
				serviceEndpoint, query);

		results = qexec.execSelect();
		
		ResultSetFormatter.out(logWriter, results, query);
		App.logger.debug(logWriter.toString());

		return results;

	}

	public ParameterizedSparqlString setPrefixes(
			ParameterizedSparqlString queryStr, List<String> prefixList) {

		for (String prefix : prefixList)
			queryStr.setNsPrefix(prefix, App.prefixMap.get(prefix));

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
