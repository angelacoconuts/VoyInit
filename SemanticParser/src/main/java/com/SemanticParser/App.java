package com.SemanticParser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.QuerySolution;
//import com.hp.hpl.jena.query.ResultSet;

public class App 
{
	protected static Logger logger = Logger.getLogger(App.class.getName());
	
    public static void main( String[] args )
    {
    	

/*    	String sparqlQueryString = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
    			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    			+ "SELECT * WHERE {"
    			+ "?subject rdf:type <http://dbpedia.org/class/yago/EuropeanCountries>."
     			+ "}";
    	String service = "http://dbpedia.org/sparql";
    	
    	AccessDBpedia query = new AccessDBpedia();
    	ResultSet results = query.execSelect( service, sparqlQueryString );
    	while(results.hasNext()){
    		QuerySolution result = results.next();
    		System.out.println(result.get("subject").toString());
    	}
//    	ResultSetFormatter.out(System.out, results, query);*/

    }
}