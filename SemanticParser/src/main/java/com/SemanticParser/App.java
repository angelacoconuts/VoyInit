package com.SemanticParser;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.search.spell.JaroWinklerDistance;

import com.sun.jersey.api.client.UniformInterfaceException;

public class App {

	protected static Logger logger = Logger.getLogger(App.class.getName());
	protected static VoyNGram entityDetector;

	public static void main(String[] args) {

		// initNeo4j();
		// test();
		// findEntity();
		// crawlPages();
		rankEntity();
		
	}

	public static void rankEntity(){
		
		VoyRank ranker = new VoyRank();
		ranker.loadEntityOccurence();
		ranker.loadAdjacentMatrix();
		
	}
	
	public static void test() {

		// final Pattern IGNORE_SUBPAGE = Pattern.compile(".*\\?.*=.*");

		// System.out.println(IGNORE_SUBPAGE.matcher("http://www.wearetraveller.com/2013/12/top-5-places-must-visit-in-south-korea.html").matches());

		/*
		 * JaroWinklerDistance JWStringDistance = new JaroWinklerDistance();
		 * JWStringDistance.setThreshold((float) 1.0);
		 * 
		 * System.out.println(JWStringDistance.getDistance("Thistle",
		 * "Thistleton"));
		 */

		/*
		try {
			System.out.println(new java.net.URI("Maple_Heights").getPath());
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
		List<String> outputTokens = new ArrayList<String>();
		outputTokens.add("STR1");
		outputTokens.add("STR2");
		outputTokens.add("STR3");
		System.out.println(StringUtils.join(outputTokens, " ") + ":");

	}

	public static void crawlPages() {

		entityDetector = new VoyNGram();

		entityDetector.loadLabelIDNameMap();
		entityDetector.loadPlaceEntitiesNGramDict();

		VoyCrawler crawler = new VoyCrawler();

		String[] seedPages = { 
			//	"http://www.wearetraveller.com/",
			//	"http://www.lonelyplanet.com/",
			//	"http://www.bbc.com/travel/",   ---do not work
			//	"http://www.travelandleisure.com/",    ---do not work
			//	"http://www.theguardian.com/travel/",   ---do not work
			//	"http://travelsguideonline.com/",
			//	"http://www.fodors.com/",
			//	"http://welovetravels.net/", 
			//	"http://greentravelerguides.com/", 
			//	"http://world66.com/",
				"http://www.worldtravelguide.net/",
				"http://www.travelchannel.com/", 
				"http://www.budgettravel.com/" };

		for (String seedPage : seedPages) {
			
			crawler.setSeedPage(seedPage);
			crawler.setNumberOfCrawlers(2);
			List<String> domainsToCrawl = Arrays.asList(seedPage);
			crawler.setDomainsToCrawl(domainsToCrawl);
			crawler.crawl();
			
		}

	}

	public static void findEntity() {

		VoyNGram ranker = new VoyNGram();

		ranker.loadLabelIDNameMap();
		ranker.loadPlaceEntitiesNGramDict();

		String str = "This one day itinerary of Hong Kong will give you some ideas of how to make the most of a very short stop in Hongkong. Everything from laidback, rural life in the outlying islands to the bustling metropolis is mentioned, and with the right planning a lot can be seen in a day! [edit]Understand Basically all you need to understand is that Hongkong is bigger and slower than most people believe. So you need to decide for yourself what you think you'd like to do in such a short time. Shopping districts can become very crowded at weekends and evenings which can make that \"short stroll down Soy Street (in Mong Kok)\" take three times as long as you planned. Just because on your map it looks like a ten-minute 3-mile bus ride doesn't mean that at 6:30pm that will be the case. [edit]Prepare The total costs for the day are around 217 HK$, not including breakfast, lunch and dinner. If you take the taxi to go to Central (see below), you should add no more than 25 HK$ NOTE: that this itinerary may try to do too much too quickly. If you are in Hongkong attempting this on a weekend or public holiday (or even just a nice day in the school holidays), bare in mind that some of the attractions mentioned here can have long queues - Especially Victoria Peak tram, and the Ngong Ping 360 cable car.[edit]Morning Get off to an early start with the 08:30am ferry service from Central to Mui Wo (New World First Ferry; HK$14.50 (ordinary single); 50 min journey; departures from Pier Number 6, [1]). Try a local style breakfast at a tsa tsan teng (茶餐廳; literally 'tea restaurant') in Mui Wo, a typical seaside town on Lantau Island which, as with most outlying island settlements, has a small expat population. The breakfast menu is a melange of Western and Chinese culinary influences. You can get a filling cooked breakfast including tea or coffee for under HK$30 per person. For the drink, try a 'yeen yeung' (half tea and half coffee) - be aware that Hong Kongers have their tea and coffee very strong (and sweet)! Breakfast ranges from macaroni in a chicken soup broth with strips of ham (火腿通粉) to instant noodles with spicy satay beef (沙爹牛肉公仔麵). Be adventurous! Catch the 10:30am bus number 2 from Mui Wo to Ngong Ping (New Lantao Bus; HK$17.20 (ordinary single); 40 min journey; departures from the main bus terminus; [2]). In Ngong Ping, visit the giant Tian Tan Buddha statue at Po Lin monastery (open daily 10am - 5:45pm; free admission). 5 minutes away is the Ngong Ping village, a themed reconstruction of traditional Chinese architecture where you can find some shops and cafes. For the slightly adventurous, you could make a short trip to Lantau peak, the highest point publicly accessible point in Hong Kong. The trail to the peak starts from behind the monastery (follow directions to the Wisdom Path, till you find signs for Lantau trail). It takes about an hour to make it to the peak and the views along the way are well worth it. Make sure you carry enough water and a jacket if it's one of the cooler months as it can get windy. From the Peak you can either hike back to the Buddha or continue on (1.5 hours) to a bus stop that will take you to back to the metro.Take the Ngong Ping 360 cable car from Ngong Ping to Tung Chung (HK$80 (ordinary single); [3]), followed by the MTR from Tung Chung to Jordan via Lai King (HK$16 (ordinary single); 40 min journey; [4]). Leave Jordan MTR station from exit D.[edit]Afternoon There is a wide selection of restaurants around Jordan MTR station. For lunch, try some Cantonese 'dim sum' or Shanghainese food. Many restaurants offer discounts for customers who enter and order after 2:30pm (which is considered to be the start of the 'afternoon tea' (下午茶) session rather than the lunch session). Spend the afternoon visiting the Hong Kong Museum of History (HK$10 (standard); HK$5 (students, children, elderly); free on Wednesdays; open 10am-6pm Mon-Sat and 10am-7pm Sun but closed on Tuesdays; [5]) which is a 10 minute walk from Jordan MTR station down Austin Road. This museum will interest everyone, even if you are not the museum-type! Don't be put off by the start of the exhbition which is about prehistoric rock formations - the exhibitions which follow get better and better. Give yourself around 2-3 hours to enjoy the 'Hong Kong Story' exhibition. 20 minutes' walk away from the Museum of History is the Star Ferry pier in Tsim Sha Tsui (alternatively it is 5 minutes away by taxi and shouldn't cost more than HK$25 for the ride for up to 4 people). Take the ferry across Victoria Harbour to Central on the upper deck (HK$2.50 (ordinary single); [6]). This short journey will give you amazing views of the CBD of Hong Kong. Back in Central, take bus 15C (New World First Bus; HK$4.20 (standard); 10 min journey; departures from just outside the Stary Ferry pier; [7]) to the Peak Tram station. The Peak Tram (HK$53 (ordinary single SkyPass ticket, including entry to Sky Terrace 428); [8]) is one of the steepest funiculars in the world and will take you through some of the most expensive residential areas of Hong Kong (and indeed the world!) to the Peak. Once on the Peak, go to the top of the Peak Tower (Sky Tower 428; entry included in the single journey SkyPass) and watch the sun set over Hong Kong. [edit]Evening Take minibus number 1 from the Peak back down to Central (HK$9.8 (ordinary single); departures every 10-15 minutes). Taking the minibus is a must if you want to have a taste of the most popular form of public transport for the locals! Get off at the stop on Peddar Street next to the Central MTR station entrance. Take the MTR to Mong Kok (HK$11 (ordinary single); 10 min journey; [9]), the part of Hong Kong where the city never rests. Here you can find plenty of cheap local eateries for supper. The streets in Mong Kok in the evenings are very crowded - no visit to Hong Kong can be complete without a personal experience of being in the midst of crowds of people on narrow streets! There are many shops selling trainers, clothing and computer products at cheap prices - you can find them if you take Exit E from the MTR station. If you still have energy left, visit the area around Temple Street in Yau Ma Tei where there are many hawker stalls to be found in the evening. This is a 15 minute walk from Mong Kok, or one stop down the Tsuen Wan Line on the MTR from Mong Kok.";

		for (String entity : ranker.findEntityInInputStr(str))
			App.logger.info(entity);

	}

	public static void generateNGram() {

		VoyNGram ranker = new VoyNGram();
		ranker.generatePlaceEntitiesDictNGram();

	}

	public static void initNeo4j() {

		VoyInit initiator = new VoyInit();

		initiator.loadGEONamesCities();

		/*
		 * initiator.fallbackPostgres(); initiator.initPostgres();
		 * initiator.initNeo4j();
		 */

		/*
		 * initiator.run(" WHERE SEQ=0");
		 * 
		 * //Almost finished except countries in VOYQUERY_DATA3
		 * initiator.run(" WHERE SEQ=1");
		 * 
		 * //Skipping for the moment initiator.run(" WHERE SEQ=2");
		 * initiator.run(" WHERE SEQ=3");
		 */

		/*
		 * Completed initiator.run(" WHERE SEQ=4");
		 */

		// Completed
		/*
		 * initiator.run(" WHERE SEQ=6");
		 */

		// initiator.run(" WHERE ID=142");

		// initiator.run(" WHERE ID IN ('122','125','128','129','130','132','141')");

		// initiator.run(" WHERE ID>119 AND ID<146");

		// initiator.run(" WHERE ID>212 AND ID<229 ORDER BY ID DESC");

		// initiator.run(" WHERE SEQ=8");

	}

}