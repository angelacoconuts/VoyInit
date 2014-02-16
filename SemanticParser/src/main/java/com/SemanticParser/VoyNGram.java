package com.SemanticParser;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.spell.JaroWinklerDistance;

public class VoyNGram {

	final String POSTGRES_GET_LABEL = "SELECT * FROM VOYLABEL ";
	final String POSTGRES_GET_LABEL_NGRAM = "SELECT * FROM VOYLABEL_NGRAM ";
	final String POSTGRES_UPDATE_LABEL_NGRAM = "INSERT INTO VOYLABEL_NGRAM VALUES";

	final String ID_COLUMN = "ID";
	final String LABEL_COLUMN = "LABEL";
	final String NGRAM_COLUMN = "NGRAM";
	final String URI_COLUMN = "URI";
	final String LABEL_ID_COLUMN = "LABEL_ID";

	public static int defaultNGramDegree = 3;
	public static int defaultShortlistCandidateNr = 60;
	public static float defaultFuzzyMatchThreshold = (float) 0.97;
	private float JWDistanceBoostThreshold = (float) 0.7;

	private int defaultDiscardExcpCommonPrefix = 2;
	private float defaultDiscardThreshold = (float) 0.8;
	private String inputContextString = "";

	public static Map<String, ArrayList<String>> labelNGramDict;
	public static Map<Integer, String> labelIDNameMap;

	private JaroWinklerDistance JWStringDistance = null;
	private AccessPostgres db = null;
	private String matchResult, nextMatchResult;
	private String[] tokens;
	private List<String> outputTokens;

	public VoyNGram() {
		db = new AccessPostgres();
		JWStringDistance = new JaroWinklerDistance();
		JWStringDistance.setThreshold(JWDistanceBoostThreshold);
	}

	public void generatePlaceEntitiesDictNGram() {

		int lowerBound = 0;
		int transactStep = 100;
		int interBound = transactStep;

		try {

			java.sql.ResultSet result = db
					.execSelect("SELECT MAX(ID) FROM VOYLABEL");
			result.next();

			int upperBound = (Integer) result.getObject("MAX");

			while (lowerBound <= upperBound) {

				App.logger.info("Decoding POIs starting from: " + lowerBound);

				decodePlaceEntitiesLabels(" WHERE ID>" + lowerBound
						+ " AND ID<=" + interBound);
				
				App.logger.info("Generating NGrams for POIs starting from: " + lowerBound);

				generatePlaceEntitiesDictNGramForRange(" WHERE ID>"
						+ lowerBound + " AND ID<=" + interBound,
						VoyNGram.defaultNGramDegree);

				interBound += transactStep;
				lowerBound += transactStep;

			}

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		}

	}

	// /Note: All comma and parenthesis in the labels will be ignored during the
	// ngram creation
	void generatePlaceEntitiesDictNGramForRange(
			String labelIDRangeCondition, int ngramDegree) {

		String label;
		int label_id;
		String updateSQL = "";

		// Pull all labels from voylabel
		java.sql.ResultSet result = db.execSelect(POSTGRES_GET_LABEL
				+ labelIDRangeCondition);

		try {

			while (result.next()) {

				label = (String) result.getObject(LABEL_COLUMN);
				label_id = (Integer) result.getObject(ID_COLUMN);
				Set<String> ngramSet = new HashSet<String>(Utils.splitNgrams(
						StringUtils.replace(label, ",", ""), ngramDegree));

				for (String ngram : ngramSet)
					updateSQL += POSTGRES_UPDATE_LABEL_NGRAM + "('"
							+ StringUtils.replace(ngram, "'", "''") + "',"
							+ label_id + "); ";

			}

			db.execUpdate(updateSQL);

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		} finally {

			db.closeResultSet(result);

		}

	}

	// /Decode the urls and labels in table voylabel
	void decodePlaceEntitiesLabels(String labelIDRangeCondition) {

		String label;
		int label_id;
		String updateSQL = "";

		// Pull all labels from voylabel
		java.sql.ResultSet result = db.execSelect(POSTGRES_GET_LABEL
				+ labelIDRangeCondition);

		try {

			while (result.next()) {

				label = (String) result.getObject(LABEL_COLUMN);
				label_id = (Integer) result.getObject(ID_COLUMN);

				if (label.contains("%"))
					try {

						label = new java.net.URI(label).getPath();

						// App.logger.info("Processing label:" + label_id + " "
						// +
						// label);

					} catch (URISyntaxException ex) {

						App.logger.error("URI Syntax Exception: ", ex);

					}

				updateSQL += "UPDATE VOYLABEL SET LABEL='"
						+ StringUtils.replace(StringUtils.replace(label, "'", "''"),"_"," ") + "' WHERE ID=" + label_id + "; ";


			}

			db.execUpdate(updateSQL);

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		} finally {

			db.closeResultSet(result);

		}

	}

	public String[] findEntityInInputStr(String input) {

		long startTime = System.currentTimeMillis();
		// List<String> EntityList = new LinkedList<String>();
		Set<String> EntitySet = new LinkedHashSet<String>();
		String[] entityNames;

		tokens = input.split("[\\s.,?!/+()\"0-9]+");

		int inputPtr = 0;
		inputContextString = "";

		while (inputPtr < tokens.length) {

			outputTokens = new ArrayList<String>();

			inputPtr = generateCaptipalizedContextChunk(inputPtr);
			
			inputContextString += StringUtils.join(outputTokens, " ") + ":";

			// App.logger.info(outputTokens.toString());

			// App.logger.info("Process one trunk time: "+
			// (System.currentTimeMillis() - startTime));

			EntitySet.addAll(findEntityInTokenChunk(
					outputTokens.toArray(new String[outputTokens.size()]),
					VoyNGram.defaultFuzzyMatchThreshold));

		}

		App.logger.info("Find entities within "
				+ (System.currentTimeMillis() - startTime) + "ms");

		entityNames = EntitySet.toArray(new String[EntitySet.size()]);

		App.logger.info(Arrays.toString(entityNames));

		return entityNames;

	}

	private int generateCaptipalizedContextChunk(int inputPtr) {

		// Add first token
		if (inputPtr < tokens.length && tokens[inputPtr].length() > 0)
			outputTokens.add(tokens[inputPtr++]);

		// Add current token as long as it is capitalized
		while (inputPtr < tokens.length && tokens[inputPtr].length() > 0
				&& Character.isUpperCase(tokens[inputPtr].charAt(0)))
			outputTokens.add(tokens[inputPtr++]);

		// Add another smaller cap token
		if (inputPtr < tokens.length && tokens[inputPtr].length() > 0)
			outputTokens.add(tokens[inputPtr++]);

		// Add current token as long as it is capitalized
		while (inputPtr < tokens.length && tokens[inputPtr].length() > 0
				&& Character.isUpperCase(tokens[inputPtr].charAt(0)))
			outputTokens.add(tokens[inputPtr++]);

		if (inputPtr < tokens.length && tokens[inputPtr].length() > 0)
			outputTokens.add(tokens[inputPtr++]);

		// Skip tokens as long as next token is lowercase
		while (inputPtr < tokens.length
				&& tokens[Math.min(inputPtr + 1, tokens.length - 1)].length() > 0
				&& !Character.isUpperCase(tokens[Math.min(inputPtr + 1,
						tokens.length - 1)].charAt(0)))
			inputPtr++;

		return inputPtr;

	}

	public List<String> findEntityInTokenChunk(String[] tokens,
			float fuzzyMatchThreshold) {

		String query, nextQuery;
		boolean shouldTakeInNextToken;
		int i, pointer = 0;
		float nextMatchScore, matchScore;
		List<String> NEList = new ArrayList<String>();

		// String[] candidateSet = findCandidateEntityDB(tokens);
		String[] candidateSet = findCandidateEntityInMemory(tokens,
				VoyNGram.defaultNGramDegree,
				VoyNGram.defaultShortlistCandidateNr);

		// long start_time = System.currentTimeMillis();

		while (pointer < tokens.length) {

			query = "";
			nextQuery = "";
			shouldTakeInNextToken = true;
			i = 0;
			nextMatchScore = 0;
			matchScore = 0;

			while (pointer + i < tokens.length && shouldTakeInNextToken) {

				nextQuery += tokens[pointer + i];
				nextMatchScore = findNearetStrMatchScore(nextQuery,
						candidateSet);

				// App.logger.info("Query: " + nextQuery + " Score: "+
				// nextMatchScore);

				if ((nextMatchScore < matchScore && JWStringDistance
						.getDistance(query, nextMatchResult) > JWStringDistance
						.getDistance(nextQuery, nextMatchResult))
						|| (nextMatchScore < this.defaultDiscardThreshold && !StringUtils
								.left(nextQuery,
										this.defaultDiscardExcpCommonPrefix)
								.equals(StringUtils.left(nextMatchResult,
										this.defaultDiscardExcpCommonPrefix))))
					shouldTakeInNextToken = false;

				else {
					query = nextQuery;
					nextQuery = nextQuery + " ";
					matchScore = nextMatchScore;
					matchResult = nextMatchResult;
					i++;
				}

			}

			if (matchScore > fuzzyMatchThreshold) {
				// Return previous token
				NEList.add(matchResult);
				pointer += i;
			} else
				pointer++;

		}

		// App.logger.info("Process time: "+ (System.currentTimeMillis() -
		// start_time));
		// if (NEList.size() > 0)
		// App.logger.info(NEList.toString());

		return NEList;

	}

	private float findNearetStrMatchScore(String query,
			String[] candidateEntitySet) {

		float matchScore = 0;
		float nextMatchScore;

		for (String candidate : candidateEntitySet) {

			nextMatchScore = JWStringDistance.getDistance(query, candidate);

			// App.logger.info("Candidate: " + candidate + " Score: "+
			// nextMatchScore);

			if (nextMatchScore > matchScore) {
				matchScore = nextMatchScore;
				nextMatchResult = candidate;
			}

		}

		return matchScore;

	}

	/*
	 * public String[] findCandidateEntityDB(String[] tokens, int ngramDegree,
	 * int candidateListLength) {
	 * 
	 * List<String> candidateLabelList = new ArrayList<String>();
	 * 
	 * // List<String> ngramList = Utils.splitNgrams( //
	 * StringUtils.replace(sentence, "'", "''"), ngramDegree);
	 * 
	 * String ngramStr = StringUtils.join( Utils.splitNgrams(tokens,
	 * ngramDegree), "','");
	 * 
	 * ResultSet resultSet = db.execSelect("SELECT LABEL" + " FROM VOYLABEL L" +
	 * " RIGHT JOIN" + " (SELECT LABEL_ID" + " FROM VOYLABEL_NGRAM" +
	 * " WHERE NGRAM IN ('" + ngramStr + "')" + " GROUP BY LABEL_ID" +
	 * " ORDER BY COUNT(1) DESC" + " LIMIT " + candidateListLength + ") N" +
	 * " ON L.ID=N.LABEL_ID");
	 * 
	 * try {
	 * 
	 * while (resultSet.next()) candidateLabelList.add((String) resultSet
	 * .getObject(LABEL_COLUMN));
	 * 
	 * } catch (SQLException ex) {
	 * 
	 * App.logger.error("SQL Exception: ", ex);
	 * 
	 * } finally {
	 * 
	 * db.closeResultSet(resultSet);
	 * 
	 * }
	 * 
	 * App.logger.info(candidateLabelList.toString());
	 * 
	 * return candidateLabelList .toArray(new
	 * String[candidateLabelList.size()]);
	 * 
	 * }
	 */

	public String[] findCandidateEntityInMemory(String[] tokens,
			int ngramDegree, int candidateListLength) {

		List<String> candidateLabelList = new ArrayList<String>();
		Map<String, Integer> labelOccurenceMap = new HashMap<String, Integer>(
				10000);

		// long startTime2 = System.currentTimeMillis();
		List<String> ngrams = Utils.splitNgrams(tokens, ngramDegree);

		for (String ngram : ngrams) {

			if (labelNGramDict.containsKey(ngram))

				for (String label : labelNGramDict.get(ngram)) {

					if (labelOccurenceMap.containsKey(label))
						labelOccurenceMap.put(label,
								labelOccurenceMap.get(label) + 1);
					else
						labelOccurenceMap.put(label, 1);
				}

		}

		// App.logger.info("Lookup memory graph time "+(System.currentTimeMillis()-startTime2));

		candidateLabelList = Utils.getMostFrequentItemInMap(labelOccurenceMap,
				candidateListLength);

		// App.logger.info("Shortlist label name time "+(System.currentTimeMillis()-startTime2));

		// App.logger.info(candidateLabelList.toString());

		return candidateLabelList
				.toArray(new String[candidateLabelList.size()]);

	}

	public void loadLabelIDNameMap() {

		String labelName;
		int labelIDKey;
		labelIDNameMap = new HashMap<Integer, String>(340000);

		long startTime = System.currentTimeMillis();

		// Load LabelID-Name map
		ResultSet resultSetLabels = db.execSelect(POSTGRES_GET_LABEL);

		try {

			while (resultSetLabels.next()) {

				labelIDKey = (Integer) resultSetLabels.getObject(ID_COLUMN);
				labelName = (String) resultSetLabels.getObject(LABEL_COLUMN);

				labelIDNameMap.put(labelIDKey, labelName);

			}

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		} finally {
			db.closeResultSet(resultSetLabels);
		}

		App.logger.info("Loaded " + labelIDNameMap.size() + " label names in "
				+ (System.currentTimeMillis() - startTime));
	}

	public void loadPlaceEntitiesNGramDict() {

		String ngramKey;
		int labelID;

		long startTime = System.currentTimeMillis();
		// Load LabelID-Ngram map
		ResultSet resultSet = db.execSelect(POSTGRES_GET_LABEL_NGRAM);

		labelNGramDict = new HashMap<String, ArrayList<String>>(45000);

		try {

			while (resultSet.next()) {

				ngramKey = (String) resultSet.getObject(NGRAM_COLUMN);
				labelID = (Integer) resultSet.getObject(LABEL_ID_COLUMN);

				if (labelNGramDict.containsKey(ngramKey))
					labelNGramDict.get(ngramKey).add(
							labelIDNameMap.get(labelID));
				else {
					ArrayList<String> labels = new ArrayList<String>();
					labels.add(labelIDNameMap.get(labelID));
					labelNGramDict.put(ngramKey, labels);
				}
			}

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		} finally {

			db.closeResultSet(resultSet);

		}

		App.logger.info("Loaded " + labelNGramDict.size() + " ngrams in "
				+ (System.currentTimeMillis() - startTime));

	}

	public String getInputContextString() {
		return inputContextString;
	}

}
