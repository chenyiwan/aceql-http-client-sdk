/*
 * This file is part of AceQL Client SDK.
 * AceQL Client SDK: Remote JDBC access over HTTP with AceQL HTTP.                                 
 * Copyright (C) 2020,  KawanSoft SAS
 * (http://www.kawansoft.com). All rights reserved.                                
 *                                                                               
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */
package com.aceql.client.jdbc;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Map;

import org.kawanfw.driver.jdbc.abstracts.AbstractResultSet;

import com.aceql.client.jdbc.http.AceQLHttpApi;
import com.aceql.client.jdbc.util.AceQLResultSetUtil;
import com.aceql.client.jdbc.util.json.RowParser;

/**
 * Class that allows to built a {@code ResultSet} from a JSON file or JSON
 * String returned by an /execute_query call.
 * 
 * @author Nicolas de Pomereu
 *
 */
class AceQLResultSet extends AbstractResultSet implements ResultSet, Closeable {

    public boolean DEBUG = false;

    /** A File containing the result set returned by an /execute_query call */
    public File jsonFile = null;

    private int rowCount = 0;
    private int currentRowNum = 0;

    // public Map<String, String> valuesPerColName;
    public Map<Integer, String> valuesPerColIndex;

    private boolean isClosed = false;

    private Statement statement = null;

    private RowParser rowParser;

    private AceQLHttpApi aceQLHttpApi = null;

    /** Says if the last accessed value was null */
    private boolean wasNull = false;

    /**
     * Constructor.
     * 
     * @param jsonFile
     *            A file containing the result set returned by an /execute_query
     *            call
     * @param statement
     *            the calling Statement
     * @param rowCount the numbers of row in the Json result set file
     * @throws SQLException
     *             if file is null or does no exist
     */
    public AceQLResultSet(File jsonFile, Statement statement, int rowCount)
	    throws SQLException {

	if (jsonFile == null) {
	    throw new SQLException("jsonFile is null!");
	}

	if (!jsonFile.exists()) {
	    throw new SQLException(new FileNotFoundException(
		    "jsonFile does not exist: " + jsonFile));
	}
	
	this.jsonFile = jsonFile;
	this.statement = statement;

	AceQLConnection aceQLConnection = (AceQLConnection) this.getStatement()
		.getConnection();
	this.aceQLHttpApi = aceQLConnection.aceQLHttpApi;

	this.rowParser = new RowParser(jsonFile);

	long begin = System.currentTimeMillis();
	debug(new java.util.Date() + " Begin getRowCount");

	this.rowCount = rowCount;

	long end = System.currentTimeMillis();
	debug(new java.util.Date() + " End getRowCount: " + rowCount);
	debug("Elapsed = " + (end - begin));
    }

    /**
     * @param row
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#absolute(int)
     */
    @Override
    public boolean absolute(int row) throws SQLException {

	if (isClosed) {
	    throw new SQLException("ResultSet is closed.");
	}

	if (row < 0 || row > rowCount) {
	    return false;
	}

	rowParser.resetParser();

	currentRowNum = row;
	rowParser.buildRowNum(row);
	return true;

    }

    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#previous()
     */
    @Override
    public boolean previous() throws SQLException {
	if (isClosed) {
	    throw new SQLException("ResultSet is closed.");
	}

	if (currentRowNum == 1) {
	    return false;
	}

	currentRowNum--;
	rowParser.buildRowNum(currentRowNum);

	valuesPerColIndex = rowParser.getValuesPerColIndex();
	// valuesPerColName = rowParser.getValuesPerColName();

	debug("");
	debug("" + valuesPerColIndex);
	// debug("" + valuesPerColName);

	return true;
    }

    @Override
    public boolean next() throws SQLException {

	if (isClosed) {
	    throw new SQLException("ResltSetWrapper is closed.");
	}

	if (currentRowNum == rowCount) {
	    return false;
	}

	currentRowNum++;
	rowParser.buildRowNum(currentRowNum);

	valuesPerColIndex = rowParser.getValuesPerColIndex();
	// valuesPerColName = rowParser.getValuesPerColName();

	debug("");
	debug("valuesPerColIndex: " + valuesPerColIndex);
	// debug("valuesPerColName :" + valuesPerColName);

	return true;

    }

    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#first()
     */
    @Override
    public boolean first() throws SQLException {
	return absolute(1);
    }

    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#last()
     */
    @Override
    public boolean last() throws SQLException {
	return absolute(rowCount);
    }

    private InputStream getInputStream(String blobId) throws SQLException {

	// long length = aceQLHttpApi.getBlobLength(blobId);
	// AceQLConnection aceQLConnection =
	// (AceQLConnection)this.getStatement().getConnection();
	// blobDownload(blobId, file, aceQLConnection.getProgress(),
	// aceQLConnection.getCancelled(), length);

	InputStream in = aceQLHttpApi.blobDownload(blobId);
	return in;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.kawanfw.driver.jdbc.abstracts.AbstractResultSet#getStatement()
     */
    @Override
    public Statement getStatement() throws SQLException {
	return this.statement;
    }

    private String getStringValue(int index) throws SQLException {

	if (isClosed) {
	    throw new SQLException("ResultSet is closed.");
	}

	String value = valuesPerColIndex.get(index);

	if (value == null) {
	    throw new SQLException("Invalid column index: " + index);
	}

	wasNull = false;
	if (value.equalsIgnoreCase("NULL")) {
	    wasNull = true;
	}

	return value;
    }

    private String getStringValue(String string) throws SQLException {

	if (isClosed) {
	    throw new SQLException("ResultSet is closed.");
	}

	if (string == null) {
	    throw new SQLException("Invalid column name: " + string);
	}

	if (rowParser.getIndexsPerColName().get(string) == null) {
	    throw new SQLException("Invalid column name: " + string);
	}

	int index = rowParser.getIndexsPerColName().get(string);

	String value = valuesPerColIndex.get(index);

	if (value == null) {
	    throw new SQLException("Invalid column name: " + string);
	}

	wasNull = false;
	if (value.equalsIgnoreCase("NULL")) {
	    wasNull = true;
	}

	return value;
    }

    /**
     * Reports whether the last column read had a value of SQL <code>NULL</code>
     * . Note that you must first call one of the getter methods on a column to
     * try to read its value and then call the method <code>wasNull</code> to
     * see if the value read was SQL <code>NULL</code>.
     * 
     * @return <code>true</code> if the last column value read was SQL
     *         <code>NULL</code> and <code>false</code> otherwise
     * @exception SQLException
     *                if a database access error occurs
     */
    @Override
    public boolean wasNull() throws SQLException {
	;
	return wasNull;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.kawanfw.driver.jdbc.abstracts.AbstractResultSet#getBinaryStream(int)
     */
    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
	String value = getString(columnIndex);

	if (value.equals("NULL")) {
	    return null;
	}
	return getInputStream(value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.kawanfw.driver.jdbc.abstracts.AbstractResultSet#getBinaryStream(java.
     * lang.String)
     */
    @Override
    public InputStream getBinaryStream(String columnName) throws SQLException {
	String value = getString(columnName);

	if (value == null || value.equals("NULL")) {
	    return null;
	}
	return getInputStream(value);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
	String value = getStringValue(columnLabel);
	if (value == null || value.equals("NULL")) {
	    return null;
	}
	return value;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
	String value = getStringValue(columnLabel);
	if (value == null || value.equals("NULL")) {
	    return 0;
	}
	return AceQLResultSetUtil.getIntValue(value);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
	String value = getStringValue(columnLabel);
	if (value == null || value.equals("NULL")) {
	    return null;
	}
	return AceQLResultSetUtil.getBigDecimalValue(value);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
	String value = getStringValue(columnLabel);

	if (value == null || value.equals("NULL")) {
	    return null;
	}
	return AceQLResultSetUtil.getDateValue(value);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
	String value = getStringValue(columnLabel);

	if (value == null || value.equals("NULL")) {
	    return null;
	}
	return AceQLResultSetUtil.getTimestampValue(value);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
	String value = getStringValue(columnLabel);
	if (value == null || value.equals("NULL")) {
	    return false;
	}

	return Boolean.parseBoolean(value);

    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
	String value = getStringValue(columnLabel);

	if (value == null || value.equals("NULL")) {
	    return 0;
	}
	return AceQLResultSetUtil.getShortValue(value);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
	String value = getStringValue(columnLabel);

	if (value == null || value.equals("NULL")) {
	    return 0;
	}
	return AceQLResultSetUtil.getFloatValue(value);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
	String value = getStringValue(columnLabel);

	if (value == null || value.equals("NULL")) {
	    return 0;
	}
	return AceQLResultSetUtil.getDoubleValue(value);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
	String value = getStringValue(columnIndex);
	if (value == null || value.equals("NULL")) {
	    return null;
	}
	return value;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
	String value = getStringValue(columnIndex);

	if (value == null || value.equals("NULL")) {
	    return 0;
	}
	return AceQLResultSetUtil.getIntValue(value);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
	String value = getStringValue(columnIndex);

	if (value == null || value.equals("NULL")) {
	    return null;
	}
	return AceQLResultSetUtil.getBigDecimalValue(value);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
	String value = getStringValue(columnIndex);

	if (value == null || value.equals("NULL")) {
	    return null;
	}
	return AceQLResultSetUtil.getDateValue(value);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
	String value = getStringValue(columnIndex);

	if (value == null || value.equals("NULL")) {
	    return null;
	}
	return AceQLResultSetUtil.getTimestampValue(value);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
	String value = getStringValue(columnIndex);
	if (value == null || value.equals("NULL")) {
	    return false;
	}
	return Boolean.parseBoolean(value);

    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
	String value = getStringValue(columnIndex);

	if (value == null || value.equals("NULL")) {
	    return 0;
	}
	return AceQLResultSetUtil.getShortValue(value);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
	String value = getStringValue(columnIndex);

	if (value == null || value.equals("NULL")) {
	    return 0;
	}
	return AceQLResultSetUtil.getFloatValue(value);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
	String value = getStringValue(columnIndex);

	if (value == null || value.equals("NULL")) {
	    return 0;
	}
	return AceQLResultSetUtil.getDoubleValue(value);
    }

    /**
     * @return
     * @throws SQLException
     * @see java.sql.ResultSet#isClosed()
     */
    @Override
    public boolean isClosed() throws SQLException {
	return isClosed;
    }

    @Override
    public void close() {
	rowParser.close();
	isClosed = true;

	if (!DEBUG) {
	    jsonFile.delete();
	}
    }

    /**
     * Says if trace is on
     * 
     * @return true if trace is on
     */
    public boolean isTraceOn() {
	return rowParser.isTraceOn();
    }

    /**
     * Sets the trace on/off
     * 
     * @param traceOn
     *            if true, trace will be on
     */
    public void setTraceOn(boolean traceOn) {
	rowParser.setTraceOn(traceOn);
    }

    private void debug(String s) {
	if (DEBUG) {
	    System.out.println(new java.util.Date() + " " + s);
	}
    }

}
