package com.SemanticParser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.la4j.matrix.Matrix;
import org.la4j.matrix.sparse.CCSMatrix;
import org.la4j.matrix.sparse.SparseMatrix;

public class VoyRank {
	
	final String GET_ENTITY_NUMBER = "SELECT MAX(ID) FROM VOYLABEL;";
	final String GET_ENTITY_LINKAGE = "SELECT LA.ID AS E1, LB.ID AS E2, LINK.DISTANCE"
			+ " FROM VOYLABEL LA, VOYLABEL LB, VOYLINKAGE LINK"
			+ " WHERE LINK.ENTITY_1=LA.LABEL"
			+ " AND LINK.ENTITY_2=LB.LABEL";
	final String GET_ENTITY_OCCURENCE = "SELECT LA.ID AS E1, OCCURENCE"
			+ " FROM VOYLABEL LA, VOYOCCURENCE LINK"
			+ " WHERE LINK.ENTITY_1=LA.LABEL;";
	final String UPDATE_ADJ_MATRIX = "INSERT INTO VOYMATRIX VALUES";
	final String ENTITY_1_ID_COLUMN = "E1";
	final String ENTITY_2_ID_COLUMN = "E2";
	final String DISTANCE_COLUMN = "DISTANCE";
	
	private AccessPostgres db = null;
	private Map<Integer, Integer> entityOccurence = new HashMap<Integer, Integer>();
	
	public VoyRank() {
		db = new AccessPostgres();
	}
	
	public void loadEntityOccurence(){
		
		java.sql.ResultSet result = null;
		
		try {
		
			long startTime = System.currentTimeMillis();

			result = db.execSelect(GET_ENTITY_OCCURENCE);
			
			while(result.next()){			

				entityOccurence.put((Integer) result.getObject(ENTITY_1_ID_COLUMN)-1, 
						(Integer) result.getObject("OCCURENCE"));
				
			}
			
			System.out.println("Load time : "+(System.currentTimeMillis() - startTime));
				

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		} finally {

			db.closeResultSet(result);

		}
		
	}
	
	public void loadAdjacentMatrix(){
		
		int N = (int) getEntityCount();
		int entity_1_ID, entity_2_ID;
		double approximityScore;
		HashMap<Integer,HashSet<Integer>> nonZeroPositions = new HashMap<Integer,HashSet<Integer>>(30000);
		
		SparseMatrix A = new CCSMatrix(N, N);
		double[] sumApproximity = new double[N];
		java.sql.ResultSet result = null;
		
		try {
		
			long startTime = System.currentTimeMillis();

			result = db.execSelect(GET_ENTITY_LINKAGE);
			
			while(result.next()){			
				
				entity_1_ID = (Integer) result.getObject(ENTITY_1_ID_COLUMN)-1;
				entity_2_ID = (Integer) result.getObject(ENTITY_2_ID_COLUMN)-1;
				approximityScore = getApproximityScore((Integer) result.getObject(DISTANCE_COLUMN), entity_1_ID);
				
				sumApproximity[entity_1_ID] += approximityScore;
				sumApproximity[entity_2_ID] += approximityScore;	
				
				A.set(entity_1_ID, entity_2_ID, A.get(entity_1_ID, entity_2_ID)+approximityScore);
				A.set(entity_2_ID, entity_1_ID, A.get(entity_2_ID, entity_1_ID)+approximityScore);
				
				if(nonZeroPositions.containsKey(entity_1_ID))
					nonZeroPositions.get(entity_1_ID).add(entity_2_ID);
				else{
					HashSet<Integer> occurences = new HashSet<Integer>();
					occurences.add(entity_2_ID);
					nonZeroPositions.put(entity_1_ID, occurences);
				}
				
				if(nonZeroPositions.containsKey(entity_2_ID))
					nonZeroPositions.get(entity_2_ID).add(entity_1_ID);
				else{
					HashSet<Integer> occurences = new HashSet<Integer>();
					occurences.add(entity_1_ID);
					nonZeroPositions.put(entity_2_ID, occurences);
				}
								
			}
			
			System.out.println("Linkage time : "+(System.currentTimeMillis() - startTime));
				

		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		} finally {

			db.closeResultSet(result);

		}
		
		for(int j : nonZeroPositions.keySet()){
			
			HashSet<Integer> occurences = nonZeroPositions.get(j);
			String updateSQL = "";
			
			for(int i : occurences){
				double normalizedValue = A.get(i, j)/sumApproximity[j];
				A.set(i, j, normalizedValue);
				updateSQL += UPDATE_ADJ_MATRIX + "("+ (i+1) + "," + (j+1) + "," + normalizedValue + "); ";
				App.logger.info(i+" - "+j+" : "+normalizedValue);
			}
			
			db.execUpdate(updateSQL);
			
		}

	}
	
	public double getApproximityScore(int distance, int entity_id){
	//	return Math.pow(distance, -0.5)/(Math.log(entityOccurence.get(entity_id))+1);
		return Math.pow(distance, -0.3)/Math.pow(entityOccurence.get(entity_id), 0.8);
	}
	
	public int getEntityCount(){
		
		int N = 0;
		
		try {

			java.sql.ResultSet result = db
					.execSelect(GET_ENTITY_NUMBER);
			result.next();

			N = (Integer) result.getObject("MAX");


		} catch (SQLException ex) {

			App.logger.error("SQL Exception: ", ex);

		}
		
		return N;
		
	}
}
