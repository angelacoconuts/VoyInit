package com.SemanticParser;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler4J extends WebCrawler {
	
	private final static Pattern IGNORE_MEDIA = Pattern
			.compile(".*(\\.(css|js|bmp|gif|jpe?g"
					+ "|png|tiff?|mid|mp2|mp3|mp4"
					+ "|wav|avi|mov|mpeg|ram|m4v|pdf"
					+ "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
	
	private final static Pattern IGNORE_SUBPAGE = Pattern.compile(".*\\?.*=.*");
	final String POSTGRES_UPDATE_VOYLINKAGE = "INSERT INTO VOYLINKAGE VALUES";
	final String POSTGRES_UPDATE_VOYPAGECONTEXT = "INSERT INTO VOYPAGECONTEXT VALUES";

	/**
	 * You should implement this function to specify whether the given url
	 * should be crawled or not (based on your crawling logic).
	 */
	@Override
	public boolean shouldVisit(WebURL url) {
		
	//	long startTime = System.currentTimeMillis();
		
	    List<String> domainsToCrawl = (List<String>) this.getMyController().getCustomData();
	    
	    String href = url.getURL().toLowerCase();
	 //   App.logger.info("Should visit? "+href);
	    
	    if (IGNORE_SUBPAGE.matcher(href).matches() || IGNORE_MEDIA.matcher(href).matches()) {
	       return false;
	    }

	    for(String domain : domainsToCrawl){
	       if (href.startsWith(domain)) {
	          return true;
	       }
	    }
	    
	//    App.logger.info("Judge should visit time: "+(System.currentTimeMillis()-startTime));
	    
	    return false;
	    
	}

	/**
	 * This function is called when a page is fetched and ready to be processed
	 * by your program.
	 */
	@Override
	public void visit(Page page) {
		
		String updateSQL = "";
		String[] entityName;
		int i,j;
	
		String url = page.getWebURL().getURL();
		App.logger.info("URL: " + url);
		url = StringUtils.replace(url, "'", "''");
		
	//    App.logger.info("Get URL time: "+(System.currentTimeMillis()-startTime));

		if (page.getParseData() instanceof HtmlParseData) {
					
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			String text = htmlParseData.getText();
			
	//	    App.logger.info("Parse raw text time: "+(System.currentTimeMillis()-startTime));	
			
			entityName = App.entityDetector.findEntityInInputStr(text);
			
	//	    App.logger.info("Find name entity time: "+(System.currentTimeMillis()-startTime));

		//	long startTime = System.currentTimeMillis();
		    
			for(j = 0 ; j < entityName.length ; j++)
				entityName[j] = StringUtils.replace(entityName[j], "'", "''");
			
			for(j = 0 ; j < entityName.length ; j++){
				for(i = j + 1 ; i < entityName.length ; i++){

					//Create relationship between entity i and entity j, distance i-j
					 updateSQL += POSTGRES_UPDATE_VOYLINKAGE + "('"
								+ entityName[j] + "','"
								+ entityName[i] + "','"
								+ url + "',"
								+ (i - j) + "); \n";
				}
			}
			
		//  App.logger.info("Generate update SQL time: "+(System.currentTimeMillis()-startTime));
			
		//	App.logger.info(updateSQL);
			
			AccessPostgres db = new AccessPostgres();
			
			updateSQL += POSTGRES_UPDATE_VOYPAGECONTEXT + "('"
						+ url + "', SUBSTRING('"
						+ StringUtils.replace(App.entityDetector.getInputContextString(), "'", "''")
						+ "' FROM 1 FOR 9990)); \n";
			
			db.execUpdate(updateSQL);
			
	   //   App.logger.info("Update DB time: "+(System.currentTimeMillis()-startTime));
			
		}
		

		
	}
}
