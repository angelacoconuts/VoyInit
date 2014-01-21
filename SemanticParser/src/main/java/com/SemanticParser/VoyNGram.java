package com.SemanticParser;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.spell.JaroWinklerDistance;

public class VoyNGram {

	static final String POSTGRES_GET_LABEL = "SELECT * FROM VOYLABEL ";
	static final String POSTGRES_GET_LABEL_NGRAM = "SELECT * FROM VOYLABEL_NGRAM ";
	static final String POSTGRES_UPDATE_LABEL_NGRAM = "INSERT INTO VOYLABEL_NGRAM VALUES";

	static final String ID_COLUMN = "ID";
	static final String LABEL_COLUMN = "LABEL";
	static final String URI_COLUMN = "URI";
	static final String LABEL_ID_COLUMN = "LABEL_ID";

	static int ngramDegree = 3;
	private String matchResult, nextMatchResult;
	static int fuzzyMatchThreshold = 2;
	static int candidateListThreshold = 20;

	private JaroWinklerDistance JWStringDistance;

	float discardThreshold = (float) 0.8;
	float acceptThreshold = (float) 0.96;

	AccessPostgres db = null;

	public VoyNGram() {
		db = new AccessPostgres();
		JWStringDistance = new JaroWinklerDistance();
	}

	public void indexPOIDict() {

		int lowerBound = 0;
		int midPointer = 10000;

		try {

			java.sql.ResultSet result = db
					.execSelect("SELECT MAX(ID) FROM VOYLABEL");
			result.next();

			int upperBound = (Integer) result.getObject("MAX");

			while (midPointer <= upperBound) {

				App.logger.info("Processing POIs starting from: " + lowerBound);

				decodeLabelForDict(" WHERE ID>" + lowerBound + " AND ID<="
						+ midPointer);

				generateNgramForDict(" WHERE ID>" + lowerBound + " AND ID<="
						+ midPointer);

				midPointer += 10000;
				lowerBound += 10000;

			}

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		}

	}

	// /Note: All comma and parenthesis in the labels will be ignored during the
	// ngram creation
	private void generateNgramForDict(String labelIDRangeCondition) {

		// Pull all labels from voylabel
		java.sql.ResultSet result = db.execSelect(POSTGRES_GET_LABEL
				+ labelIDRangeCondition);

		try {

			while (result.next()) {

				String label = (String) result.getObject(LABEL_COLUMN);
				int label_id = (Integer) result.getObject(ID_COLUMN);
				Set<String> ngramSet = new HashSet<String>(Utils.splitNgrams(
						StringUtils.replace(label, ",", ""), ngramDegree));

				for (String ngram : ngramSet)
					db.execUpdate(POSTGRES_UPDATE_LABEL_NGRAM + "('"
							+ StringUtils.replace(ngram, "'", "''") + "',"
							+ label_id + ");");

			}

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		} finally {

			db.closeResultSet(result);

		}

	}

	// /Decode the urls and labels in table voylabel
	private void decodeLabelForDict(String labelIDRangeCondition) {

		// Pull all labels from voylabel
		java.sql.ResultSet result = db.execSelect(POSTGRES_GET_LABEL
				+ labelIDRangeCondition);

		try {

			while (result.next()) {

				String label = (String) result.getObject(LABEL_COLUMN);
				int label_id = (Integer) result.getObject(ID_COLUMN);

				try {

					label = new java.net.URI(label).getPath();

					// App.logger.info("Processing label:" + label_id + " " +
					// label);

				} catch (URISyntaxException ex) {

					App.logger.error("URI Syntax Exception: ", ex);

				}

				db.execUpdate("UPDATE VOYLABEL SET LABEL='"
						+ StringUtils.replace(
								StringUtils.replace(label, "'", "''"), "_", " ")
						+ "' WHERE ID=" + label_id);

			}

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		} finally {

			db.closeResultSet(result);

		}

	}

	public void chunkInputStream(String input) {

		long startTime = System.currentTimeMillis();

		String processed = StringUtils.replace(input, "'", " ");
		String[] tokens = processed.split("[\\s.,?!/+()\"0-9]+");

		int inputPtr = 0;

		while (inputPtr < tokens.length) {

			List<String> outputTokens = new ArrayList<String>();

			// Add first token
			if (tokens[inputPtr].length() > 0)
				outputTokens.add(tokens[inputPtr]);

			// Add current token as long as next token is capitalized
			while (inputPtr < tokens.length - 1
					&& tokens[Math.min(inputPtr + 1, tokens.length - 1)]
							.length() > 0
					&& Character.isUpperCase(tokens[Math.min(inputPtr + 1,
							tokens.length - 1)].charAt(0)))
				outputTokens.add(tokens[1 + inputPtr++]);

			// Add another smaller cap token
			if (++inputPtr < tokens.length && tokens[inputPtr].length() > 0)
				outputTokens.add(tokens[inputPtr]);

			while (inputPtr < tokens.length - 1
					&& tokens[Math.min(inputPtr + 1, tokens.length - 1)]
							.length() > 0
					&& Character.isUpperCase(tokens[Math.min(inputPtr + 1,
							tokens.length - 1)].charAt(0)))
				outputTokens.add(tokens[1 + inputPtr++]);

			// Skip tokens as long as next token is lowercase
			while (inputPtr < tokens.length
					&& tokens[Math.min(inputPtr + 1, tokens.length - 1)]
							.length() > 0
					&& !Character.isUpperCase(tokens[Math.min(inputPtr + 1,
							tokens.length - 1)].charAt(0)))
				inputPtr++;

			App.logger.info(outputTokens.toString());
			
			
			App.logger.info("Process one trunk time: "
					+ (System.currentTimeMillis() - startTime));
			
			// getPOIInChunk(outputTokens.toArray(new
			// String[outputTokens.size()]));
			findNearestPOI(outputTokens
					.toArray(new String[outputTokens.size()]));

		}

		App.logger.info("Total time: "
				+ (System.currentTimeMillis() - startTime));

	}

	public List<String> findNearestPOI(String[] tokens) {

		List<String> NEList = new ArrayList<String>();

		List<String> candidateSet = findCandidateEntitySet(tokens);

		int pointer = 0;
		// long start_time = System.currentTimeMillis();

		while (pointer < tokens.length) {

			String query = "", nextQuery = "";
			boolean shouldTakeInNextToken = true;
			int i = 0;
			float nextMatchScore = 0, matchScore = 0;

			while (pointer + i < tokens.length && shouldTakeInNextToken) {

				nextQuery += tokens[pointer + i];
				nextMatchScore = findMaximumMatchScore(nextQuery, candidateSet);
				
				App.logger.info("Query: " + nextQuery + " Score: " + nextMatchScore);

				if ((nextMatchScore < matchScore && JWStringDistance
						.getDistance(query, nextMatchResult) > JWStringDistance
						.getDistance(nextQuery, nextMatchResult))
						|| (nextMatchScore < discardThreshold && !StringUtils
								.left(nextQuery, 2).equals(
										StringUtils.left(nextMatchResult, 2))))
					shouldTakeInNextToken = false;
				else {
					query = nextQuery;
					nextQuery = nextQuery + " ";
					matchScore = nextMatchScore;
					matchResult = nextMatchResult;
					i++;
				}

			}

			if (matchScore > acceptThreshold) {
				// Return previous token
				NEList.add(matchResult);
				pointer += i;
			} else
				pointer++;

		}

		// App.logger.info("Process time: "+ (System.currentTimeMillis() -
		// start_time));
		App.logger.info(NEList.toString());

		return NEList;

	}

	private float findMaximumMatchScore(String query,
			List<String> candidateEntitySet) {

		float matchScore = 0;

		for (String candidate : candidateEntitySet) {

			float nextMatchScore = JWStringDistance.getDistance(query,
					candidate);
			
			App.logger.info("Candidate: " + candidate + " Score: " + nextMatchScore);

			if (nextMatchScore > matchScore) {
				matchScore = nextMatchScore;
				nextMatchResult = candidate;
			}

		}

		return matchScore;

	}

	public List<String> getPOIInChunk(String[] tokens) {

		List<String> candidateSet;
		List<String> POIList = new ArrayList<String>();

		long start_time = System.currentTimeMillis();

		candidateSet = findCandidateEntitySet(tokens);

		App.logger.info("Query postgres for candidates: "
				+ (System.currentTimeMillis() - start_time));

		int pointer = 0;
		while (pointer < tokens.length) {

			String query = tokens[pointer];
			int distance = getShortestDistance(query, candidateSet);
			int i = 1;
			int next_distance = 0;
			boolean shouldTakeInNextToken = true;
			matchResult = nextMatchResult;

			while (pointer + i < tokens.length && shouldTakeInNextToken) {

				String next_query = query + " " + tokens[pointer + i];
				next_distance = getShortestDistance(next_query, candidateSet);

				if (next_distance > distance || pointer + i >= tokens.length)
					shouldTakeInNextToken = false;

				else {
					// Accept current token
					distance = next_distance;
					query = next_query;
					matchResult = nextMatchResult;
					// Continue take in next token
					i++;
				}

			}

			if (distance <= fuzzyMatchThreshold) {
				// Return previous token
				POIList.add(matchResult);
				pointer += i;

			} else
				pointer++;

		}

		App.logger.info("Process time: "
				+ (System.currentTimeMillis() - start_time));
		App.logger.info(POIList.toString());

		return POIList;

	}

	private int getShortestDistance(String query,
			List<String> candidateEntitySet) {

		int distance = 500;

		for (String candidate : candidateEntitySet) {

			int next_distance = StringUtils.getLevenshteinDistance(query,
					candidate);

			if (next_distance < distance) {
				distance = next_distance;
				nextMatchResult = candidate;
			}

		}

		return distance;

	}

	public List<String> findCandidateEntitySet(String[] tokens) {

		List<String> candidateLabelList = new ArrayList<String>();

		// List<String> ngramList = Utils.splitNgrams(
		// StringUtils.replace(sentence, "'", "''"), ngramDegree);

		String ngramStr = StringUtils.join(
				Utils.splitNgrams(tokens, ngramDegree), "','");

		ResultSet resultSet = db.execSelect("SELECT LABEL" + " FROM VOYLABEL L"
				+ " RIGHT JOIN" + " (SELECT LABEL_ID" + " FROM VOYLABEL_NGRAM"
				+ " WHERE NGRAM IN ('" + ngramStr + "')" + " GROUP BY LABEL_ID"
				+ " ORDER BY COUNT(1) DESC" + " LIMIT "
				+ candidateListThreshold + ") N" + " ON L.ID=N.LABEL_ID");

		try {

			while (resultSet.next())
				candidateLabelList.add((String) resultSet
						.getObject(LABEL_COLUMN));

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		} finally {

			db.closeResultSet(resultSet);

		}

		App.logger.info(candidateLabelList.toString());
		return candidateLabelList;

	}

}
