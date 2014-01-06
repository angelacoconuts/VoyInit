package com.SemanticParser;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;

public class AccessNeo4j {

	private static final String NEO4J_SERVER_URI = "http://localhost:7474/db/data/";
	private static final String CypherEntryURI = NEO4J_SERVER_URI + "cypher";
	private static final String NodeEntryURI = NEO4J_SERVER_URI + "node";
	private static final String IndexEntryURI = NEO4J_SERVER_URI
			+ "schema/index/";
	private static final String ConstraintEntryURI = NEO4J_SERVER_URI
			+ "schema/constraint/";

	/**
	 * Create simple node
	 * 
	 * @return
	 */
	public URI createNode() {

		ClientResponse response = postToEndpoint(NodeEntryURI, "{}");

		URI nodeLocation = response.getLocation();

		if (response != null)
			response.close();

		return nodeLocation;
	}

	/**
	 * Create node with label and attributes
	 * 
	 * @param label
	 *            e.g: poi
	 * @param attributes
	 *            attribute list in json format e.g:
	 *            {"uri":"http...","label_en":"China"}
	 */
	public URI createNode(String nodeLabel, String JSONNodeProperties) {

		String cypherCreateNodeStr = "{ \"query\":" + "\"CREATE (n:"
				+ nodeLabel + " { props } ) RETURN n\" , " + "\"params\":"
				+ "{\"props\":" + JSONNodeProperties + "}}";

		ClientResponse response = postToEndpoint(CypherEntryURI,
				cypherCreateNodeStr);
		
		String responseStr = response.getEntity(String.class);

		ArrayList<String> strResult = (ArrayList<String>) getCypherResponseAttribute(
				JSONObject.fromObject(responseStr), "self", String.class);

		if (strResult.size() != 0) {

			String nodeLocationStr = strResult.get(0);
			App.logger.debug("New node location :" + nodeLocationStr);
			response.close();

			return URI.create(nodeLocationStr);
		}

		response.close();
		return null;

	}

	/**
	 * Call neo4j endpoint and execute cypher query
	 * 
	 * @param queryString
	 * @return
	 */
	public JSONObject executeCypherQuery(String queryString) {

		String cypherQueryStr = new JSONObject().element("query", queryString)
				.element("params", "{}").toString();

		ClientResponse response = postToEndpoint(CypherEntryURI, cypherQueryStr);

		String responseStr = response.getEntity(String.class);
		// App.logger.info(responseStr);

		if (response != null)
			response.close();

		return JSONObject.fromObject(responseStr);

	}

	/**
	 * Parse cypher query response data and get named attribute
	 * 
	 * @param responseJSONObject
	 *            cypher query response in JSONObject
	 * @param attribute
	 *            name of attribute to be retrieved
	 * @param returnType
	 *            return type, e.g:String.class, JSONObject.class
	 * @return
	 */
	public <T> List<T> getCypherResponseAttribute(
			JSONObject responseJSONObject, String attribute, Class<T> returnType) {

		List<T> objectList = new ArrayList<T>();

		JSONArray array = (JSONArray) responseJSONObject.get("data");
		for (int i = 0; i < array.size(); i++) {

			JSONArray par_element = array.getJSONArray(i);
			JSONObject element = par_element.getJSONObject(0);
			objectList.add((T) element.get(attribute));

		}

		return objectList;
	}

	/**
	 * Add property to node or relationship
	 * 
	 * @param resourceURI
	 *            node/relationship URI
	 * @param propertyKey
	 * @param propertyValue
	 */
	public void addProperty(URI resourceURI, String propertyKey,
			String propertyValue) {

		String propertyURI = resourceURI.toString() + "/properties/"
				+ propertyKey;

		WebResource resource = Client.create().resource(propertyURI);
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON)
				.entity("\"" + propertyValue + "\"").put(ClientResponse.class);

		App.logger.debug(String.format("PUT to [%s], status code [%d]",
				propertyURI, response.getStatus()));

		if (response.getStatus() >= 300) {
			App.logger.error(String.format("PUT to [%s], status code [%d]",
					propertyURI, response.getStatus()));
			throw new UniformInterfaceException(
					"Status code indicates request not expected", response);
		}

		if (response != null)
			response.close();

	}

	/**
	 * Create index on list of attributes upon nodes tagged with label
	 * 
	 * @param label
	 *            e.g: poi
	 * @param attributes
	 *            comma delimited attribute list e.g: uri
	 */
	public void createIndex(String label, String attributes) {

		String indexURI = IndexEntryURI + label;

		ClientResponse response = postToEndpoint(indexURI,
				"{ \"property_keys\" : [ \"" + attributes + "\" ] }");

		if (response != null)
			response.close();
	}

	/**
	 * Create uniqueness constraint on list of attributes upon nodes tagged with
	 * label
	 * 
	 * @param label
	 *            e.g: poi
	 * @param attributes
	 *            comma delimited attribute list e.g: "uri","label_en"
	 */
	public void createUniqueConstraint(String label, String attributes) {

		String uniqueConstraintURI = ConstraintEntryURI + label
				+ "/uniqueness/";

		ClientResponse response = postToEndpoint(uniqueConstraintURI,
				"{ \"property_keys\" : [ " + attributes + " ] }");

		if (response != null)
			response.close();

	}

	/**
	 * Create relationship with list of attributes
	 * 
	 * @param fromNode
	 * @param toNode
	 * @param relType
	 * @param JSONproperties
	 *            properties list in json format e.g: {"parent":"country"}
	 */
	public void addRelationship(URI fromNode, URI toNode, String relType,
			String JSONRelProperties) {

		String relJSONString = new JSONObject()
				.element("to", toNode.toString()).element("type", relType)
				.element("data", JSONRelProperties).toString();

		ClientResponse response = postToEndpoint(fromNode.toString()
				+ "/relationships", relJSONString);

		if (response != null)
			response.close();

	}

	public void testServerConnection() {

		ClientResponse response = null;
		WebResource resource = Client.create().resource(NEO4J_SERVER_URI);

		response = resource.get(ClientResponse.class);

		App.logger.debug(String.format("GET on [%s], status code [%d]",
				NEO4J_SERVER_URI, response.getStatus()));

		if (response != null)
			response.close();

	}

	private ClientResponse postToEndpoint(String postEndpoint,
			String postContent) {

		WebResource resource = Client.create().resource(postEndpoint);
		ClientResponse response = null;

		response = resource.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON).entity(postContent)
				.post(ClientResponse.class);

		App.logger.debug(String.format(
				"POST to [%s], status code [%d], post content [%s]",
				postEndpoint, response.getStatus(), postContent));

		if (response.getStatus() >= 300) {
			App.logger.error(String.format(
					"POST to [%s], status code [%d], post content [%s]",
					postEndpoint, response.getStatus(), postContent));
			throw new UniformInterfaceException(
					"Status code indicates request not expected", response);
		}

		return response;

	}
}
