package com.SemanticParser;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;

public class App {

	protected static Logger logger = Logger.getLogger(App.class.getName());

	public static void main(String[] args) {

		VoyInit initiator = new VoyInit();
		initiator.run();

	}

}