package com.SemanticParser;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.search.spell.JaroWinklerDistance;

public class App {

	protected static Logger logger = Logger.getLogger(App.class.getName());

	public static void main(String[] args) {

		VoyNGram ranker = new VoyNGram();

		List<List<String>> outputTokens = new ArrayList<List<String>>();
		
	//	ranker.indexPOIDict();
	//	List<String> candidate = ranker.findCandidateEntitySet("the Ngong Ping village".split(" "));
		
	//	for(String str : candidate)
	//		System.out.println(str);
		
		String str = "This one day itinerary of Hong Kong will give you some ideas of how to make the most of a very short stop in Hongkong.";	
		ranker.chunkInputStream(str);
		
	//	JaroWinklerDistance distance = new JaroWinklerDistance();
	//	System.out.println(distance.getDistance("Tung Chung", "Tung Chung Fort"));
		
	}
	
	public void initNeo4j(){
		
	//	VoyInit initiator = new VoyInit();
		
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

	//	initiator.run(" WHERE ID=142");
		
	//	initiator.run(" WHERE ID IN ('122','125','128','129','130','132','141')");
		
	//	initiator.run(" WHERE ID>119 AND ID<146");
		
	//	initiator.run(" WHERE ID>212 AND ID<229 ORDER BY ID DESC");		
		
	//	initiator.run(" WHERE SEQ=8");
		
	}

}