package com.SemanticParser;

import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.ibatis.jdbc.ScriptRunner;

public class AccessPostgres {

	private String DBurl = "jdbc:postgresql://localhost/voydb";
	private String DBuser = "angelacoconuts";
	private String DBpassword = "voypsw";
    
    public ResultSet execSelect(String queryString){
    	
    	Connection con = getConnection();
        Statement st = null;
        ResultSet result = null;
        
        try {
        	
            st = con.createStatement();
            result = st.executeQuery(queryString);

            if (result.isBeforeFirst()) {
            	App.logger.info("Retrieve successfully from "+DBurl);
            	App.logger.debug("SQL Query: "+queryString);
            }
            
            return result;

        } catch (SQLException ex) {
        	App.logger.error(ex.getMessage());

        } finally {
            try {
                if (result != null) {
                    result.close();
                }
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }

            } catch (SQLException ex) {
            	App.logger.error(ex.getMessage());
            }
        }
		return null;
    }
    
    /**
     * Execute a SQL script
     * No ResultSet return!
     * @param reader
     */
    public void execScript(Reader reader){
    	
    	Connection con = getConnection();
        
        try {

        	ScriptRunner runner=new ScriptRunner(con);
        	runner.runScript(reader);

        } finally {
            try {
                if (con != null) {
                    con.close();
                }

            } catch (SQLException ex) {
            	App.logger.error(ex.getMessage());
            }
        }
    }
    
	public int execUpdate(String updateString){
    	
    	Connection con = getConnection();
        Statement st = null;
        int result = -1;
        
        try {

            st = con.createStatement();
            result = st.executeUpdate(updateString);

            if (result >= 0) {
            	App.logger.info("Update successful to"+DBurl);
            	App.logger.debug("SQL Query: "+updateString);
            }
            return result;

        } catch (SQLException ex) {
        	App.logger.error(ex.getMessage());

        } finally {
            try {
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }

            } catch (SQLException ex) {
            	App.logger.error(ex.getMessage());
            }
        }
		return result;
    }
	
    private Connection getConnection() {
		// TODO Auto-generated method stub
    	Connection con = null;
        try {
        	
			con = DriverManager.getConnection(DBurl, DBuser, DBpassword);
	    	App.logger.debug("Connected successfully to "+DBurl);
	    	
	    	return con;
	    	
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
		return null;
	}

	public String getDBurl() {
		return DBurl;
	}
	public void setDBurl(String dBurl) {
		DBurl = dBurl;
	}
	public String getDBuser() {
		return DBuser;
	}
	public void setDBuser(String dBuser) {
		DBuser = dBuser;
	}
	public String getDBpassword() {
		return DBpassword;
	}
	public void setDBpassword(String dBpassword) {
		DBpassword = dBpassword;
	}
}
