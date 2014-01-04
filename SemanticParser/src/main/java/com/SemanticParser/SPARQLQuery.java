package com.SemanticParser;

public class SPARQLQuery {
	
	private String queryStr = null;
	
	private String[] prefixes = null;
	
	private int isLeaveQuery = 0;
	
	private String inputVariable = null;
	
	private String inputVariableValue = null;
	
	private String[] outputVariables = null;
	
	private String[] outputRelations = null;

	public String getQueryStr() {
		return queryStr;
	}

	public void setQueryStr(String queryStr) {
		this.queryStr = queryStr;
	}

	public String[] getPrefixes() {
		return prefixes;
	}

	public void setPrefixes(String[] prefixes) {
		this.prefixes = prefixes;
	}

	public int getIsLeaveQuery() {
		return isLeaveQuery;
	}

	public void setIsLeaveQuery(int isLeaveQuery) {
		this.isLeaveQuery = isLeaveQuery;
	}

	public String getInputVariable() {
		return inputVariable;
	}

	public void setInputVariable(String inputVariable) {
		this.inputVariable = inputVariable;
	}

	public String[] getOutputVariables() {
		return outputVariables;
	}

	public void setOutputVariables(String[] outputVariables) {
		this.outputVariables = outputVariables;
	}

	public String[] getOutputRelations() {
		return outputRelations;
	}

	public void setOutputRelations(String[] outputRelations) {
		this.outputRelations = outputRelations;
	}

	public String getInputVariableValue() {
		return inputVariableValue;
	}

	public void setInputVariableValue(String inputVariableValue) {
		this.inputVariableValue = inputVariableValue;
	}
	
}
