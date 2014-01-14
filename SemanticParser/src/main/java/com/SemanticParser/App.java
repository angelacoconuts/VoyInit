package com.SemanticParser;

import org.apache.log4j.Logger;

public class App {

	protected static Logger logger = Logger.getLogger(App.class.getName());

	public static void main(String[] args) {

		VoyInit initiator = new VoyInit();
		
		/*
		initiator.fallbackPostgres();
		initiator.initPostgres();
		initiator.initNeo4j();
		*/		
		
		/*
		initiator.run(" WHERE SEQ=0");
		
		//Almost finished except countries in VOYQUERY_DATA3
		initiator.run(" WHERE SEQ=1");
		
		//Skipping for the moment
		initiator.run(" WHERE SEQ=2");
		initiator.run(" WHERE SEQ=3");
		*/		
		
		/*Completed
		initiator.run(" WHERE SEQ=4");
		*/
		
		//Completed
		/*
		initiator.run(" WHERE SEQ=6");
		*/

		initiator.run(" WHERE ID in (95,96,116,117) OR (ID>118 AND ID<146)");
		
		initiator.run(" WHERE ID>212 AND ID<229 ORDER BY ID DESC");		
		
		initiator.run(" WHERE SEQ=8");


	}

}