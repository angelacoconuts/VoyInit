package com.SemanticParser;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
//import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

public class App {
	
	protected static Logger logger = Logger.getLogger(App.class.getName());
	
	public static void main(String[] args) {

//		fallbackPostgres();
//		initPostgres();
		
//		VoyInit initiator = new VoyInit();
//		initiator.run();
		
	}

	public static boolean initPostgres() {

		AccessPostgres db = new AccessPostgres();
		String createTblFile = "/home/angelacoconuts/Documents/dev/git/VoyInit/create_table.sql";
		String initTblFile = "/home/angelacoconuts/Documents/dev/git/VoyInit/init_table.sql";

		try {

			db.execScript(new BufferedReader(new FileReader(createTblFile)));
			db.execScript(new BufferedReader(new FileReader(initTblFile)));

			return true;

		} catch (FileNotFoundException ex) {

			App.logger.equals(ex.getMessage());
			App.logger.error("FileNotFoundException: ", ex);

		}

		return false;

	}
	
	public static boolean fallbackPostgres() {

		AccessPostgres db = new AccessPostgres();
		String fallbackTblFile = "/home/angelacoconuts/Documents/dev/git/VoyInit/fallback_table.sql";

		try {

			db.execScript(new BufferedReader(new FileReader(fallbackTblFile)));

			return true;

		} catch (FileNotFoundException ex) {

			App.logger.equals(ex.getMessage());
			App.logger.error("FileNotFoundException: ", ex);

		}

		return false;

	}
}