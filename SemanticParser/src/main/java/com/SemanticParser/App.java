package com.SemanticParser;

import java.sql.Connection;
import java.sql.DriverManager;
//import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

public class App 
{
	protected static Logger logger = Logger.getLogger(App.class.getName());
	
    public static Map<String, String> prefixMap = new HashMap<String, String>();
    static{
            prefixMap.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
            prefixMap.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
            prefixMap.put("dbpedia-owl", "http://dbpedia.org/ontology/");
    }
	
    public static void main( String[] args )
    {
    	

    	String sparqlQueryString = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
    			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
    			+ "SELECT * WHERE {"
    			+ "?subject rdf:type <http://dbpedia.org/class/yago/EuropeanCountries>."
     			+ "}";
    	String service = "http://dbpedia.org/sparql";
    	
    	AccessDBpedia query = new AccessDBpedia();
    	List<String> prefixList = new ArrayList<String>();
    	prefixList.add("rdf");
    	prefixList.add("rdfs");    	
		ResultSet results = query.execSimpleSelect( service, sparqlQueryString, prefixList );
    	while(results.hasNext()){
    		QuerySolution result = results.next();
    		System.out.println(result.get("subject").toString());
    	}

    }
}