package com.SemanticParser;


import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;


public class AccessNeo4jEmbedded {
	
	private static final String NEO4J_DB_PATH = "/usr/local/neo4j/data/graph.db/";
	GraphDatabaseService graphDB;
	Index<Node> labelIndex;
	Index<Node> URIIndex;
    public static enum RelTypes implements RelationshipType
    {
        BELONGS
    }
	
	public AccessNeo4jEmbedded(){
		
	//	createDB();
	//	labelIndex = createIndex( "label_en" );
	//	URIIndex = createIndex( "uri" );
		
	}
	
	public void createDB(){
		
		graphDB = new GraphDatabaseFactory().newEmbeddedDatabase( NEO4J_DB_PATH );
		App.logger.info("Start embedded graphDB");
		
	}
	
	public void shutdownDB(GraphDatabaseService graphDB){
		
		graphDB.shutdown();
		App.logger.info("Shut down graphDB");
		
	}
	
	public Index<Node> createIndex(String nodesName){
		
		Transaction tx = graphDB.beginTx();
		
		IndexManager index = graphDB.index();
		Index<Node> nodes = index.forNodes( nodesName );
		
		tx.success();
		
		return nodes;
		
	}
	
}
