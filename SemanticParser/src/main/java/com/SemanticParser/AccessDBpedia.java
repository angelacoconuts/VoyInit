package com.SemanticParser;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;

public class AccessDBpedia {
	
	private String testEndpointQuery = "ASK { }";
	
	/**
	 * Fire a SPARQL against service endpoint
	 * @param serviceEndpoint SPARQL service endpoint e.g: http://dbpedia.org/sparql
	 * @param sparqlQueryString  SPARQL query string
	 * @return com.hp.hpl.jena.query.ResultSet
	 */
	public ResultSet execSelect(String serviceEndpoint, String sparqlQueryString){
	
    	Query query = QueryFactory.create(sparqlQueryString);
    	
    	QueryExecution qexec = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        
    	ResultSet results = qexec.execSelect();
    	
    	return results;
    	
 	}
	
	/**
	 * Test whether a service endpoint is up and running or not
	 * @param serviceEndpoint
	 * @return test result (boolean)
	 */
	public boolean testEndpoint(String serviceEndpoint){
		
    	Query query = QueryFactory.create(testEndpointQuery);
    	
    	QueryExecution qexec = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        
    	try{
    		if(qexec.execAsk()){
    			App.logger.info(serviceEndpoint + " is UP!");
    			return true;
    		}
     	} catch (QueryExceptionHTTP e){
			App.logger.error(serviceEndpoint + " is DOWN!");
			return false;
     	}
		return false;
    	
 	}
}
