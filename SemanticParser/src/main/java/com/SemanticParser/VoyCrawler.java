package com.SemanticParser;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class VoyCrawler {

	private CrawlConfig crawlConfig;
	private String crawlStorageFolder = "/home/angelacoconuts/Documents/dev/git/VoyInit/data";
	private int numberOfCrawlers = 1;
	private int maxDepthOfCrawling = 5;
	private int maxPagesToFetch = 10000;
	private List<String> domainsToCrawl = Arrays.asList("http://wikitravel.org/en/");
	private String seedPage = "http://wikitravel.org/en/";
	
	public void crawl(){
		
		setCrawlConfig();
	
        PageFetcher pageFetcher = new PageFetcher(this.crawlConfig);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        CrawlController controller;
        
		try {
			
			controller = new CrawlController(this.crawlConfig, pageFetcher, robotstxtServer);
			
			controller.addSeed(this.seedPage);
						
			controller.setCustomData(domainsToCrawl);
	        
	        /*
	         * Start the crawl. This is a blocking operation, meaning that your code
	         * will reach the line after this only when crawling is finished.
	         */
	        controller.start(Crawler4J.class, this.numberOfCrawlers);
	        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			App.logger.error("Crawler exception", e);
		}
        
	}
	
	private void setCrawlConfig(){
		
		crawlConfig = new CrawlConfig();
		crawlConfig.setCrawlStorageFolder(this.crawlStorageFolder);    
		crawlConfig.setMaxDepthOfCrawling(this.maxDepthOfCrawling);
		crawlConfig.setMaxPagesToFetch(this.maxPagesToFetch);
	//	crawlConfig.setResumableCrawling(true);
		
	}

	public String getCrawlStorageFolder() {
		return crawlStorageFolder;
	}

	public void setCrawlStorageFolder(String crawlStorageFolder) {
		this.crawlStorageFolder = crawlStorageFolder;
	}

	public int getNumberOfCrawlers() {
		return numberOfCrawlers;
	}

	public void setNumberOfCrawlers(int numberOfCrawlers) {
		this.numberOfCrawlers = numberOfCrawlers;
	}

	public int getMaxDepthOfCrawling() {
		return maxDepthOfCrawling;
	}

	public void setMaxDepthOfCrawling(int maxDepthOfCrawling) {
		this.maxDepthOfCrawling = maxDepthOfCrawling;
	}

	public int getMaxPagesToFetch() {
		return maxPagesToFetch;
	}

	public void setMaxPagesToFetch(int maxPagesToFetch) {
		this.maxPagesToFetch = maxPagesToFetch;
	}

	public List<String> getDomainsToCrawl() {
		return domainsToCrawl;
	}

	public void setDomainsToCrawl(List<String> domainsToCrawl) {
		this.domainsToCrawl = domainsToCrawl;
	}

	public String getSeedPage() {
		return seedPage;
	}

	public void setSeedPage(String seedPage) {
		this.seedPage = seedPage;
	}
	
}
