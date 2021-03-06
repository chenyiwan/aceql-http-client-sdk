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
package com.aceql.client.jdbc.http;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.ProtocolException;
import java.net.Proxy;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.kawanfw.sql.version.VersionValues;

import com.aceql.client.jdbc.AceQLException;
import com.aceql.client.jdbc.util.UserLoginStore;
import com.aceql.client.jdbc.util.json.SqlParameter;
import com.aceql.client.metadata.dto.JdbcDatabaseMetaDataDto;
import com.aceql.client.metadata.dto.TableDto;
import com.aceql.client.metadata.dto.TableNamesDto;
import com.aceql.client.metadata.util.GsonWsUtil;

/**
 * @author Nicolas de Pomereu
 *
 *         AceQL Rest wrapper for AceQL http/REST apis that take care of all
 *         http calls and operations.
 *
 *         All Exceptions are trapped with a {#link AceQLException} that allows
 *         to retrieve the detail of the Exceptions
 */
public class AceQLHttpApi {

    public static boolean DEBUG = false;

    private boolean TRACE_ON = false;

    // private values
    private String serverUrl;
    private String username;
    private char[] password;
    private String sessionId;
    private String database;

    /** Proxy to use with HttpUrlConnection */
    private Proxy proxy = null;
    /** For authenticated proxy */
    private PasswordAuthentication passwordAuthentication = null;

    private static int connectTimeout = 0;
    private static int readTimeout = 0;

    /** Always true and can not be changed */
    private final boolean prettyPrinting = true;

    private boolean gzipResult = true;

    private String url = null;
    private int httpStatusCode = HttpURLConnection.HTTP_OK;
    private String httpStatusMessage;

    private AtomicBoolean cancelled;
    private AtomicInteger progress;


    /**
     * Sets the read timeout.
     *
     * @param readTimeout an <code>int</code> that specifies the read timeout value,
     *                    in milliseconds, to be used when an http connection is
     *                    established to the remote server. See
     *                    {@link URLConnection#setReadTimeout(int)}
     */
    public static void setReadTimeout(int readTimeout) {
	AceQLHttpApi.readTimeout = readTimeout;
    }

    /**
     * Sets the connect timeout.
     *
     * @param connectTimeout Sets a specified timeout value, in milliseconds, to be
     *                       used when opening a communications link to the remote
     *                       server. If the timeout expires before the connection
     *                       can be established, a java.net.SocketTimeoutException
     *                       is raised. A timeout of zero is interpreted as an
     *                       infinite timeout. See
     *                       {@link URLConnection#setConnectTimeout(int)}
     */
    public static void setConnectTimeout(int connectTimeout) {
	AceQLHttpApi.connectTimeout = connectTimeout;
    }

    /**
     * Login on the AceQL server and connect to a database
     *
     * @param serverUrl              the url of the AceQL server. Example:
     *                               http://localhost:9090/aceql
     * @param database               the server database to connect to.
     * @param username               the login
     * @param password               the password
     * @param proxy                  the proxy to use. null if none.
     * @param passwordAuthentication the username and password holder to use for
     *                               authenticated proxy. Null if no proxy or if
     *                               proxy
     * @throws AceQLException if any Exception occurs
     * @deprecated Use {@link #AceQLHttpApi(String,String,String,char[],String,Proxy,PasswordAuthentication)} instead
     */
    @Deprecated
    public AceQLHttpApi(String serverUrl, String database, String username, char[] password, Proxy proxy,
            PasswordAuthentication passwordAuthentication) throws AceQLException {
        	this(serverUrl, database, username, password, null, proxy, passwordAuthentication);
            }

    /**
     * Login on the AceQL server and connect to a database
     *
     * @param serverUrl              the url of the AceQL server. Example:
     *                               http://localhost:9090/aceql
     * @param database               the server database to connect to.
     * @param username               the login
     * @param password               the password
     * @param sessionId		     the session ID, if no password authentication
     * @param proxy                  the proxy to use. null if none.
     * @param passwordAuthentication the username and password holder to use for
     *                               authenticated proxy. Null if no proxy or if
     *                               proxy
     * @throws AceQLException if any Exception occurs
     */
    public AceQLHttpApi(String serverUrl, String database, String username, char[] password, String sessionId,
	    Proxy proxy, PasswordAuthentication passwordAuthentication) throws AceQLException {

	try {
	    if (serverUrl == null) {
		throw new NullPointerException("serverUrl is null!");
	    }
	    if (database == null) {
		throw new NullPointerException("database is null!");
	    }
	    if (username == null) {
		throw new NullPointerException("username is null!");
	    }

	    if (password == null && sessionId == null) {
		throw new NullPointerException("password and sessionId are both null!");
	    }

	    this.serverUrl = serverUrl;
	    this.username = username;
	    this.database = database;
	    this.password = password;
	    this.sessionId = sessionId;

	    this.proxy = proxy;
	    this.passwordAuthentication = passwordAuthentication;

	    setProxyCredentials();

	    /*
	     * BEGIN OLD implementation with GET String url = serverUrl + "/database/" +
	     * database + "/username/" + username + "/login" + "?password=" + new
	     * String(password) + "&stateless=" + stateless;
	     *
	     * String result = callWithGet(url);
	     *
	     * trace("result: " + result); END OLD implementation with GET
	     */


	    UserLoginStore userLoginStore = new UserLoginStore(serverUrl, username, database);

            if (sessionId != null)
            {
                userLoginStore.setSessionId(sessionId);
            }

	    if (userLoginStore.isAlreadyLogged()) {
		trace("Get a new connection with get_connection");
		sessionId = userLoginStore.getSessionId();

		String theUrl = serverUrl + "/session/" + sessionId + "/get_connection";
		String result = callWithGet(theUrl);

		trace("result: " + result);

		ResultAnalyzer resultAnalyzer = new ResultAnalyzer(result, httpStatusCode, httpStatusMessage);

		if (!resultAnalyzer.isStatusOk()) {
		    throw new AceQLException(resultAnalyzer.getErrorMessage(), resultAnalyzer.getErrorType(), null,
			    resultAnalyzer.getStackTrace(), httpStatusCode);
		}

		String connectionId = resultAnalyzer.getValue("connection_id");
		trace("Ok. New Connection created: " + connectionId);

		this.url = serverUrl + "/session/" + sessionId + "/connection/" + connectionId + "/";

	    } else {
		String url = serverUrl + "/database/" + database + "/username/" + username + "/login";

		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("password", new String(password));
		parameters.put("client_version", VersionValues.VERSION);

		String result = callWithPostReturnString(new URL(url), parameters);

		trace("result: " + result);

		ResultAnalyzer resultAnalyzer = new ResultAnalyzer(result, httpStatusCode, httpStatusMessage);

		if (!resultAnalyzer.isStatusOk()) {
		    throw new AceQLException(resultAnalyzer.getErrorMessage(), resultAnalyzer.getErrorType(), null,
			    resultAnalyzer.getStackTrace(), httpStatusCode);
		}

		trace("Ok. Connected! ");
		sessionId = resultAnalyzer.getValue("session_id");
		String connectionId = resultAnalyzer.getValue("connection_id");
		trace("sessionId   : " + sessionId);
		trace("connectionId: " + connectionId);

		this.url = serverUrl + "/session/" + sessionId + "/connection/" + connectionId + "/";

		userLoginStore.setSessionId(sessionId);
	    }

	} catch (Exception e) {
	    if (e instanceof AceQLException) {
		throw (AceQLException) e;
	    } else {
		throw new AceQLException(e.getMessage(), 0, e, null, httpStatusCode);
	    }
	}

    }

    public void trace() {
	if (TRACE_ON) {
	    System.out.println();
	}
    }

    public void trace(String s) {
	if (TRACE_ON) {
	    System.out.println(s);
	}
    }

    private void callApiNoResult(String commandName, String commandOption) throws AceQLException {

	try {

	    if (commandName == null) {
		throw new NullPointerException("commandName is null!");
	    }

	    String result = callWithGet(commandName, commandOption);

	    ResultAnalyzer resultAnalyzer = new ResultAnalyzer(result, httpStatusCode, httpStatusMessage);
	    if (!resultAnalyzer.isStatusOk()) {
		throw new AceQLException(resultAnalyzer.getErrorMessage(), resultAnalyzer.getErrorType(), null,
			resultAnalyzer.getStackTrace(), httpStatusCode);
	    }

	} catch (Exception e) {
	    if (e instanceof AceQLException) {
		throw (AceQLException) e;
	    } else {
		throw new AceQLException(e.getMessage(), 0, e, null, httpStatusCode);
	    }
	}
    }

    private String callApiWithResult(String commandName, String commandOption) throws AceQLException {

	try {

	    if (commandName == null) {
		throw new NullPointerException("commandName is null!");
	    }

	    String result = callWithGet(commandName, commandOption);

	    ResultAnalyzer resultAnalyzer = new ResultAnalyzer(result, httpStatusCode, httpStatusMessage);
	    if (!resultAnalyzer.isStatusOk()) {
		throw new AceQLException(resultAnalyzer.getErrorMessage(), resultAnalyzer.getErrorType(), null,
			resultAnalyzer.getStackTrace(), httpStatusCode);
	    }

	    return resultAnalyzer.getResult();

	} catch (Exception e) {

	    if (e instanceof AceQLException) {
		throw (AceQLException) e;
	    } else {
		throw new AceQLException(e.getMessage(), 0, e, null, httpStatusCode);
	    }
	}
    }

    private String callWithGet(String action, String actionParameter) throws IOException {

	String urlWithaction = url + action;

	if (actionParameter != null && !actionParameter.isEmpty()) {
	    urlWithaction += "/" + actionParameter;
	}

	return callWithGet(urlWithaction);

    }

    // FUTUR USAGE: HTTP/2 with HttpClient

    // private int httpVersion = 1;
    // OkHttpClient client = new OkHttpClient();
    //
    // private InputStream callWithGetInputStreamHttp2(String url)
    // throws MalformedURLException, IOException, ProtocolException {
    //
    // Request request = new Request.Builder().url(url).build();
    //
    // Response response = client.newCall(request).execute();
    // return response.body().byteStream();
    //
    // }

    private InputStream callWithGetReturnStream(String url)
	    throws MalformedURLException, IOException, UnsupportedEncodingException {

	/*
	 * if (httpVersion == 1) { return callWithGetInputStreamHttp11(url); } else {
	 * return callWithGetInputStreamHttp2(url); }
	 */

	return callWithGetInputStreamHttp11(url);

    }

    private InputStream callWithGetInputStreamHttp11(String url)
	    throws MalformedURLException, IOException, ProtocolException {
	URL theUrl = new URL(url);
	HttpURLConnection conn = null;

	if (this.proxy == null) {
	    conn = (HttpURLConnection) theUrl.openConnection();
	} else {
	    conn = (HttpURLConnection) theUrl.openConnection(proxy);
	}

	conn.setRequestProperty("Accept-Charset", "UTF-8");
	conn.setReadTimeout(readTimeout);
	conn.setRequestMethod("GET");
	conn.setDoOutput(true);

	trace();
	trace("Executing request " + url);

	httpStatusCode = conn.getResponseCode();
	httpStatusMessage = conn.getResponseMessage();

	InputStream in = null;
	// if (httpStatusCode == HttpURLConnection.HTTP_OK || httpStatusCode ==
	// HttpURLConnection.HTTP_MOVED_TEMP) {
	if (httpStatusCode == HttpURLConnection.HTTP_OK) {
	    in = conn.getInputStream();
	} else {
	    in = conn.getErrorStream();
	}

	return in;
    }

    private String callWithGet(String url)
	    throws MalformedURLException, IOException, ProtocolException, UnsupportedEncodingException {

	String responseBody;

	try (InputStream in = callWithGetReturnStream(url)) {
	    if (in == null)
		return null;

	    ByteArrayOutputStream out = new ByteArrayOutputStream();

	    IOUtils.copy(in, out);

	    responseBody = out.toString("UTF-8");
	    if (responseBody != null) {
		responseBody = responseBody.trim();
	    }

	    trace("----------------------------------------");
	    trace(responseBody);
	    trace("----------------------------------------");

	    return responseBody;
	}

    }

    /*
     * NO! Bad implementation: always call an URL private InputStream
     * callWithPost(String action, Map<String, String> parameters) throws
     * MalformedURLException, IOException, ProtocolException,
     * UnsupportedEncodingException {
     *
     * URL theUrl = new URL(url + action); return callWithPost(theUrl, parameters);
     * }
     */

    private InputStream callWithPost(URL theUrl, Map<String, String> parameters)
	    throws IOException, ProtocolException, SocketTimeoutException, UnsupportedEncodingException {
	HttpURLConnection conn = null;

	if (this.proxy == null) {
	    conn = (HttpURLConnection) theUrl.openConnection();
	} else {
	    conn = (HttpURLConnection) theUrl.openConnection(proxy);
	}

	conn.setRequestProperty("Accept-Charset", "UTF-8");
	conn.setReadTimeout(readTimeout);
	conn.setRequestMethod("POST");
	conn.setDoOutput(true);

	TimeoutConnector timeoutConnector = new TimeoutConnector(conn, connectTimeout);

	try (OutputStream connOut = timeoutConnector.getOutputStream();) {
	    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(connOut, "UTF-8"));
	    writer.write(AceQLHttpApi.getPostDataString(parameters));

	    // writer.flush();
	    writer.close();
	}

	trace();
	trace("Executing request: " + theUrl.toString());

	if (parameters.containsKey("sql")) {
	    trace("sql..............: " + parameters.get("sql"));
	}

	trace("parameters.......: " + parameters);

	// Analyze the error after request execution
	httpStatusCode = conn.getResponseCode();
	httpStatusMessage = conn.getResponseMessage();

	InputStream in = null;
	if (httpStatusCode == HttpURLConnection.HTTP_OK) {
	    in = conn.getInputStream();
	} else {
	    in = conn.getErrorStream();
	}

	return in;
    }

    private void setProxyCredentials() {

	if (proxy == null) {
	    return;
	}

	// Sets the credential for authentication
	if (passwordAuthentication != null) {
	    final String proxyAuthUsername = passwordAuthentication.getUserName();
	    final char[] proxyPassword = passwordAuthentication.getPassword();

	    Authenticator authenticator = new Authenticator() {

		@Override
		public PasswordAuthentication getPasswordAuthentication() {
		    return new PasswordAuthentication(proxyAuthUsername, proxyPassword);
		}
	    };

	    if (DEBUG) {
		System.out.println("passwordAuthentication: " + proxyAuthUsername + " " + new String(proxyPassword));
	    }

	    Authenticator.setDefault(authenticator);
	}

    }

    // ////////////////////////////////////////////////////
    // PUBLIC METHODS //
    // ///////////////////////////////////////////////////

    @Override
    public AceQLHttpApi clone() {
	AceQLHttpApi aceQLHttpApi;
	try {
	    aceQLHttpApi = new AceQLHttpApi(serverUrl, database, username, password, sessionId, proxy, passwordAuthentication);
	    aceQLHttpApi.setGzipResult(gzipResult);
	} catch (SQLException e) {
	    throw new IllegalStateException(e);
	}
	return aceQLHttpApi;
    }

    /**
     * Says if trace is on
     *
     * @return true if trace is on
     */
    public boolean isTraceOn() {
	return TRACE_ON;
    }

    /**
     * Sets the trace on/off
     *
     * @param TRACE_ON if true, trace will be on
     */
    public void setTraceOn(boolean traceOn) {
	TRACE_ON = traceOn;
    }

    /**
     * Returns the cancelled value set by the progress indicator
     *
     * @return the cancelled value set by the progress indicator
     */
    public AtomicBoolean getCancelled() {
	return cancelled;
    }

    /**
     * Sets the shareable canceled variable that will be used by the progress
     * indicator to notify this instance that the user has cancelled the current
     * blob/clob upload or download.
     *
     * @param cancelled the shareable canceled variable that will be used by the
     *                  progress indicator to notify this instance that the end user
     *                  has cancelled the current blob/clob upload or download
     *
     */
    public void setCancelled(AtomicBoolean cancelled) {
	this.cancelled = cancelled;
    }

    /**
     * Returns the sharable progress variable that will store blob/clob upload or
     * download progress between 0 and 100
     *
     * @return the sharable progress variable that will store blob/clob upload or
     *         download progress between 0 and 100
     *
     */
    public AtomicInteger getProgress() {
	return progress;
    }

    /**
     * Sets the sharable progress variable that will store blob/clob upload or
     * download progress between 0 and 100. Will be used by progress indicators to
     * show the progress.
     *
     * @param progress the sharable progress variable
     */
    public void setProgress(AtomicInteger progress) {
	this.progress = progress;
    }

    /**
     * Says the query result is returned compressed with the GZIP file format.
     *
     * @return the gzipResult
     */
    public boolean isGzipResult() {
	return gzipResult;
    }

    /**
     * Define if result sets are compressed before download. Defaults to true.
     *
     * @param gzipResult if true, sets are compressed before download
     */
    public void setGzipResult(boolean gzipResult) {
	this.gzipResult = gzipResult;
    }

    /**
     * Calls /get_version API
     *
     * @throws AceQLException if any Exception occurs
     */
    public String getServerVersion() throws AceQLException {
	String result = callApiWithResult("get_version", null);
	return result;
    }

    /**
     * Gets the SDK version
     *
     * @throws AceQLException if any Exception occurs
     */
    public String getClientVersion() {
	return org.kawanfw.sql.version.Version.getVersion();
    }

    /**
     * Calls /close API
     */
    public void close() throws AceQLException {
	callApiNoResult("close", null);
    }

    /**
     * Calls /logout API
     *
     * @throws AceQLException if any Exception occurs
     */
    public void logout() throws AceQLException {
	UserLoginStore loginStore = new UserLoginStore(serverUrl, username, database);
	loginStore.remove();
	callApiNoResult("logout", null);
    }

    /**
     * Calls /commit API
     *
     * @throws AceQLException if any Exception occurs
     */
    public void commit() throws AceQLException {
	callApiNoResult("commit", null);
    }

    /**
     * Calls /rollback API
     *
     * @throws AceQLException if any Exception occurs
     */
    public void rollback() throws AceQLException {
	callApiNoResult("rollback", null);
    }

    /**
     * Calls /set_transaction_isolation_level API
     *
     * @param level the isolation level
     * @throws AceQLException if any Exception occurs
     */
    public void setTransactionIsolation(String level) throws AceQLException {
	callApiNoResult("set_transaction_isolation_level", level);
    }

    /**
     * Calls /set_holdability API
     *
     * @param holdability the holdability
     * @throws AceQLException if any Exception occurs
     */
    public void setHoldability(String holdability) throws AceQLException {
	callApiNoResult("set_holdability", holdability);
    }

    /**
     * Calls /set_auto_commit API
     *
     * @param autoCommit <code>true</code> to enable auto-commit mode;
     *                   <code>false</code> to disable it
     * @throws AceQLException if any Exception occurs
     */
    public void setAutoCommit(boolean autoCommit) throws AceQLException {
	callApiNoResult("set_auto_commit", autoCommit + "");
    }

    /**
     * Calls /get_auto_commit API
     *
     * @param autoCommit <code>true</code> to enable auto-commit mode;
     *                   <code>false</code> to disable it
     * @return the current state of this <code>Connection</code> object's
     *         auto-commit mode
     * @throws AceQLException if any Exception occurs
     */
    public boolean getAutoCommit() throws AceQLException {
	String result = callApiWithResult("get_auto_commit", null);
	return Boolean.parseBoolean(result);
    }

    /**
     * Calls /is_read_only API
     *
     * @return <code>true</code> if this <code>Connection</code> object is
     *         read-only; <code>false</code> otherwise
     * @throws AceQLException if any Exception occurs
     */
    public boolean isReadOnly() throws AceQLException {
	String result = callApiWithResult("is_read_only", null);
	return Boolean.parseBoolean(result);
    }

    /**
     * Calls /set_read_only API
     *
     * @param readOnly {@code true} enables read-only mode; {@code false} disables
     *                 it
     * @throws AceQLException if any Exception occurs
     */
    public void setReadOnly(boolean readOnly) throws AceQLException {
	callApiNoResult("set_read_only", readOnly + "");
    }

    /**
     * Calls /get_holdability API
     *
     * @return the holdability, one of <code>hold_cursors_over_commit</code> or
     *         <code>close_cursors_at_commit</code>
     * @throws AceQLException if any Exception occurs
     */
    public String getHoldability() throws AceQLException {
	String result = callApiWithResult("get_holdability", null);
	return result;
    }

    /**
     * Calls /get_transaction_isolation_level API
     *
     * @return the current transaction isolation level, which will be one of the
     *         following constants: <code>transaction_read_uncommitted</code>,
     *         <code>transaction_read_committed</code>,
     *         <code>transaction_repeatable_read</code>,
     *         <code>transaction_serializable</code>, or
     *         <code>transaction_none</code>.
     * @throws AceQLException if any Exception occurs
     */
    public String getTransactionIsolation() throws AceQLException {
	String result = callApiWithResult("get_transaction_isolation_level", null);
	return result;
    }

    /**
     * Calls /execute_update API
     *
     * @param sql                   an SQL <code>INSERT</code>, <code>UPDATE</code>
     *                              or <code>DELETE</code> statement or an SQL
     *                              statement that returns nothing
     * @param isPreparedStatement   if true, the server will generate a prepared
     *                              statement, else a simple statement
     * @param isStoredProcedure     TODO
     * @param statementParameters   the statement parameters in JSON format. Maybe
     *                              null for simple statement call.
     * @param callableOutParameters the map of OUT parameters
     * @return either the row count for <code>INSERT</code>, <code>UPDATE</code> or
     *         <code>DELETE</code> statements, or <code>0</code> for SQL statements
     *         that return nothing
     * @throws AceQLException if any Exception occurs
     */
    public int executeUpdate(String sql, boolean isPreparedStatement, boolean isStoredProcedure,
	    Map<String, String> statementParameters, Map<Integer, SqlParameter> callableOutParameters)
	    throws AceQLException {

	try {
	    if (sql == null) {
		throw new NullPointerException("sql is null!");
	    }

	    String action = "execute_update";

	    // Call raw execute if non query/select stored procedure. (Dirty!! To be
	    // corrected.)
	    if (isStoredProcedure) {
		action = "execute";
	    }

	    Map<String, String> parametersMap = new HashMap<String, String>();
	    parametersMap.put("sql", sql);

	    // parametersMap.put("prepared_statement", new Boolean(
	    // isPreparedStatement).toString());
	    parametersMap.put("prepared_statement", "" + isPreparedStatement);
	    parametersMap.put("stored_procedure", "" + isStoredProcedure);

	    trace("sql: " + sql);
	    trace("statement_parameters: " + statementParameters);

	    // Add the statement parameters map
	    if (statementParameters != null) {
		parametersMap.putAll(statementParameters);
	    }

	    URL theUrl = new URL(url + action);

	    String result = callWithPostReturnString(theUrl, parametersMap);

	    ResultAnalyzer resultAnalyzer = new ResultAnalyzer(result, httpStatusCode, httpStatusMessage);
	    if (!resultAnalyzer.isStatusOk()) {
		throw new AceQLException(resultAnalyzer.getErrorMessage(), resultAnalyzer.getErrorType(), null,
			resultAnalyzer.getStackTrace(), httpStatusCode);
	    }

	    if (isStoredProcedure) {
		updateOutParameters(resultAnalyzer, callableOutParameters);
	    }

	    int rowCount = resultAnalyzer.getIntvalue("row_count");
	    return rowCount;

	} catch (Exception e) {
	    if (e instanceof AceQLException) {
		throw (AceQLException) e;
	    } else {
		throw new AceQLException(e.getMessage(), 0, e, null, httpStatusCode);
	    }
	}

    }

    /**
     * Update the Map of callable OUT parameters using the result string in
     * ResultAnalyzer
     *
     * @param resultAnalyzer        the JSON container sent by the server afte the
     *                              update
     * @param callableOutParameters the OUT parameters to update after the execute.
     * @throws AceQLException if server does not return awaited OUT parameters
     */
    private static synchronized void updateOutParameters(ResultAnalyzer resultAnalyzer,
	    Map<Integer, SqlParameter> callableOutParameters) throws AceQLException {

	// Immediate return in case no out parameters set by user
	if (callableOutParameters == null || callableOutParameters.isEmpty()) {
	    return;
	}

	Map<Integer, String> parametersOutPerIndexAfterExecute = resultAnalyzer.getParametersOutPerIndex();

	// Immediate return in case no parameters. This can not happen if
	// callableOutParameters is not empty
	if (parametersOutPerIndexAfterExecute == null || parametersOutPerIndexAfterExecute.isEmpty()) {
	    throw new AceQLException("No stored procedure out parameters returned by AceQL Server", 4, null, null,
		    HttpURLConnection.HTTP_OK);
	}

	for (Integer key : callableOutParameters.keySet()) {
	    if (parametersOutPerIndexAfterExecute.containsKey(key)) {
		SqlParameter sqlParameter = callableOutParameters.get(key);
		SqlParameter sqlParameterNew = new SqlParameter(key, sqlParameter.getParameterType(),
			parametersOutPerIndexAfterExecute.get(key));
		// Put back new value
		callableOutParameters.put(key, sqlParameterNew);
	    }
	}

    }

    private String callWithPostReturnString(URL theUrl, Map<String, String> parametersMap)
	    throws IOException, ProtocolException, SocketTimeoutException, UnsupportedEncodingException {

	String result = null;

	try (InputStream in = callWithPost(theUrl, parametersMap);) {

	    if (in != null) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		IOUtils.copy(in, out);

		result = out.toString("UTF-8");
		trace("result: " + result);
	    }
	}
	return result;
    }

    /**
     * Calls /execute_query API
     *
     * @param sql                 an SQL <code>INSERT</code>, <code>UPDATE</code> or
     *                            <code>DELETE</code> statement or an SQL statement
     *                            that returns nothing
     * @param isPreparedStatement if true, the server will generate a prepared
     *                            statement, else a simple statement
     * @param isStoredProcedure   TODO
     * @param statementParameters the statement parameters in JSON format. Maybe
     *                            null for simple statement call.
     * @return the input stream containing either an error, or the result set in
     *         JSON format. See user documentation.
     * @throws AceQLException if any Exception occurs
     */
    public InputStream executeQuery(String sql, boolean isPreparedStatement, boolean isStoredProcedure,
	    Map<String, String> statementParameters) throws AceQLException {

	try {
	    if (sql == null) {
		throw new NullPointerException("sql is null!");
	    }

	    String action = "execute_query";

	    Map<String, String> parametersMap = new HashMap<String, String>();
	    parametersMap.put("sql", sql);

	    // parametersMap.put("prepared_statement", new
	    // Boolean(isPreparedStatement).toString());
	    // parametersMap.put("gzip_result", new
	    // Boolean(gzipResult).toString());
	    // parametersMap.put("pretty_printing", new
	    // Boolean(prettyPrinting).toString());

	    parametersMap.put("prepared_statement", "" + isPreparedStatement);
	    parametersMap.put("stored_procedure", "" + isStoredProcedure);
	    parametersMap.put("gzip_result", "" + gzipResult);
	    parametersMap.put("pretty_printing", "" + prettyPrinting);

	    // Add the statement parameters map
	    if (statementParameters != null) {
		parametersMap.putAll(statementParameters);
	    }

	    trace("sql: " + sql);
	    trace("statement_parameters: " + statementParameters);

	    URL theUrl = new URL(url + action);
	    InputStream in = callWithPost(theUrl, parametersMap);
	    return in;

	} catch (Exception e) {
	    if (e instanceof AceQLException) {
		throw (AceQLException) e;
	    } else {
		throw new AceQLException(e.getMessage(), 0, e, null, httpStatusCode);
	    }
	}

    }

    /**
     * Calls /blob_upload API
     *
     * @param blobId      the Blob/Clob Id
     * @param inputStream the local Blob/Clob local file input stream
     * @throws AceQLException if any Exception occurs
     */
    public void blobUpload(String blobId, InputStream inputStream, long totalLength) throws AceQLException {

	try {
	    if (blobId == null) {
		throw new NullPointerException("blobId is null!");
	    }

	    if (inputStream == null) {
		throw new NullPointerException("inputStream is null!");
	    }

	    // if (file == null) {
	    // throw new NullPointerException("file is null!");
	    // }
	    //
	    // if (!file.exists()) {
	    // throw new FileNotFoundException("file does not exist: " + file);
	    // }

	    URL theURL = new URL(url + "blob_upload");

	    trace("request : " + theURL);
	    HttpURLConnection conn = null;

	    if (proxy == null) {
		conn = (HttpURLConnection) theURL.openConnection();
	    } else {
		conn = (HttpURLConnection) theURL.openConnection(proxy);
	    }

	    conn.setRequestProperty("Accept-Charset", "UTF-8");
	    conn.setRequestMethod("POST");
	    conn.setReadTimeout(readTimeout);
	    conn.setDoOutput(true);

	    final MultipartUtility http = new MultipartUtility(theURL, conn, connectTimeout, progress, cancelled,
		    totalLength);

	    Map<String, String> parameters = new HashMap<String, String>();
	    parameters.put("blob_id", blobId);

	    for (Map.Entry<String, String> entry : parameters.entrySet()) {
		// trace(entry.getKey() + "/" + entry.getValue());
		http.addFormField(entry.getKey(), entry.getValue());
	    }

	    // Server needs a unique file name to store the blob
	    String fileName = UUID.randomUUID().toString() + ".blob";

	    http.addFilePart("file", inputStream, fileName);
	    http.finish();

	    conn = http.getConnection();

	    // Analyze the error after request execution
	    httpStatusCode = conn.getResponseCode();
	    httpStatusMessage = conn.getResponseMessage();

	    trace("blob_id          : " + blobId);
	    trace("httpStatusCode   : " + httpStatusCode);
	    trace("httpStatusMessage: " + httpStatusMessage);

	    InputStream inConn = null;

	    String result;

	    if (httpStatusCode == HttpURLConnection.HTTP_OK) {
		inConn = conn.getInputStream();
	    } else {
		inConn = conn.getErrorStream();
	    }

	    result = null;

	    if (inConn != null) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		IOUtils.copy(inConn, out);
		result = out.toString("UTF-8");
	    }

	    ResultAnalyzer resultAnalyzer = new ResultAnalyzer(result, httpStatusCode, httpStatusMessage);
	    if (!resultAnalyzer.isStatusOk()) {
		throw new AceQLException(resultAnalyzer.getErrorMessage(), resultAnalyzer.getErrorType(), null,
			resultAnalyzer.getStackTrace(), httpStatusCode);
	    }

	} catch (Exception e) {
	    if (e instanceof AceQLException) {
		throw (AceQLException) e;
	    } else {
		throw new AceQLException(e.getMessage(), 0, e, null, httpStatusCode);
	    }
	}
    }

    /**
     * Calls /get_blob_length API
     *
     * @param blobId the Blob/Clob Id
     * @return the server Blob/Clob length
     * @throws AceQLException if any Exception occurs
     */
    public long getBlobLength(String blobId) throws AceQLException {

	try {

	    if (blobId == null) {
		throw new NullPointerException("blobId is null!");
	    }

	    String action = "get_blob_length";

	    Map<String, String> parameters = new HashMap<String, String>();
	    parameters.put("blob_id", blobId);

	    ByteArrayOutputStream out = new ByteArrayOutputStream();

	    String result = null;

	    // try (InputStream in = callWithPost(action, parameters);) {
	    //
	    // if (in != null) {
	    // IOUtils.copy(in, out);
	    // result = out.toString("UTF-8");
	    // }
	    // }

	    InputStream in = null;
	    try {
		URL theUrl = new URL(url + action);
		in = callWithPost(theUrl, parameters);
		if (in != null) {
		    IOUtils.copy(in, out);
		    result = out.toString("UTF-8");
		}
	    } finally {
		if (in != null) {
		    try {
			in.close();
		    } catch (Exception ignore) {
			// ignore
		    }
		}
	    }

	    ResultAnalyzer resultAnalyzer = new ResultAnalyzer(result, httpStatusCode, httpStatusMessage);

	    if (!resultAnalyzer.isStatusOk()) {
		throw new AceQLException(resultAnalyzer.getErrorMessage(), resultAnalyzer.getErrorType(), null,
			resultAnalyzer.getStackTrace(), httpStatusCode);
	    }

	    String lengthStr = resultAnalyzer.getValue("length");
	    long length = Long.parseLong(lengthStr);
	    return length;

	} catch (Exception e) {
	    if (e instanceof AceQLException) {
		throw (AceQLException) e;
	    } else {
		throw new AceQLException(e.getMessage(), 0, e, null, httpStatusCode);
	    }
	}

    }

    public InputStream dbSchemaDownload(String format, String tableName) throws AceQLException {
	try {

	    if (format == null) {
		throw new NullPointerException("format is null!");
	    }

	    String action = "metadata_query/db_schema_download";

	    Map<String, String> parameters = new HashMap<String, String>();
	    parameters.put("format", format);
	    if (tableName != null) {
		tableName = tableName.toLowerCase();
		parameters.put("table_name", tableName);
	    }

	    InputStream in = null;

	    URL theUrl = new URL(url + action);

	    in = callWithPost(theUrl, parameters);
	    return in;

	} catch (Exception e) {
	    if (e instanceof AceQLException) {
		throw (AceQLException) e;
	    } else {
		throw new AceQLException(e.getMessage(), 0, e, null, httpStatusCode);
	    }
	}

    }

    public JdbcDatabaseMetaDataDto getDbMetadata() throws AceQLException {
	try {
	    String action = "metadata_query/get_db_metadata";
	    String result = callWithGet(url + action);

	    ResultAnalyzer resultAnalyzer = new ResultAnalyzer(result, httpStatusCode, httpStatusMessage);
	    if (!resultAnalyzer.isStatusOk()) {
		throw new AceQLException(resultAnalyzer.getErrorMessage(), resultAnalyzer.getErrorType(), null,
			resultAnalyzer.getStackTrace(), httpStatusCode);
	    }

	    // If result is OK, it's a DTO
	    JdbcDatabaseMetaDataDto jdbcDatabaseMetaDataDto = GsonWsUtil.fromJson(result,
		    JdbcDatabaseMetaDataDto.class);
	    return jdbcDatabaseMetaDataDto;
	} catch (Exception e) {
	    if (e instanceof AceQLException) {
		throw (AceQLException) e;
	    } else {
		throw new AceQLException(e.getMessage(), 0, e, null, httpStatusCode);
	    }
	}
    }

    public TableNamesDto getTableNames(String tableType) throws AceQLException {
	try {
	    String action = "metadata_query/get_table_names";

	    Map<String, String> parameters = new HashMap<String, String>();
	    if (tableType != null) {
		parameters.put("table_type", tableType);
	    }

	    String result = callWithPostReturnString(new URL(url + action), parameters);

	    ResultAnalyzer resultAnalyzer = new ResultAnalyzer(result, httpStatusCode, httpStatusMessage);
	    if (!resultAnalyzer.isStatusOk()) {
		throw new AceQLException(resultAnalyzer.getErrorMessage(), resultAnalyzer.getErrorType(), null,
			resultAnalyzer.getStackTrace(), httpStatusCode);
	    }

	    // If result is OK, it's a DTO
	    TableNamesDto tableNamesDto = GsonWsUtil.fromJson(result, TableNamesDto.class);
	    return tableNamesDto;
	} catch (Exception e) {
	    if (e instanceof AceQLException) {
		throw (AceQLException) e;
	    } else {
		throw new AceQLException(e.getMessage(), 0, e, null, httpStatusCode);
	    }
	}
    }

    public TableDto getTable(String tableName) throws AceQLException {
	try {
	    String action = "metadata_query/get_table";

	    Map<String, String> parameters = new HashMap<String, String>();
	    parameters.put("table_name", tableName);

	    String result = callWithPostReturnString(new URL(url + action), parameters);

	    ResultAnalyzer resultAnalyzer = new ResultAnalyzer(result, httpStatusCode, httpStatusMessage);
	    if (!resultAnalyzer.isStatusOk()) {
		throw new AceQLException(resultAnalyzer.getErrorMessage(), resultAnalyzer.getErrorType(), null,
			resultAnalyzer.getStackTrace(), httpStatusCode);
	    }

	    // If result is OK, it's a DTO
	    TableDto tableDto = GsonWsUtil.fromJson(result, TableDto.class);
	    return tableDto;
	} catch (Exception e) {
	    if (e instanceof AceQLException) {
		throw (AceQLException) e;
	    } else {
		throw new AceQLException(e.getMessage(), 0, e, null, httpStatusCode);
	    }
	}
    }

    /**
     * Calls /blob_download API
     *
     * @param blobId the Blob/Clob Id
     * @return the input stream containing either an error, or the result set in
     *         JSON format. See user documentation.
     * @throws AceQLException if any Exception occurs
     */
    public InputStream blobDownload(String blobId) throws AceQLException {

	try {

	    if (blobId == null) {
		throw new NullPointerException("blobId is null!");
	    }

	    String action = "blob_download";

	    Map<String, String> parameters = new HashMap<String, String>();
	    parameters.put("blob_id", blobId);

	    InputStream in = null;

	    URL theUrl = new URL(url + action);
	    in = callWithPost(theUrl, parameters);

	    // if (httpStatusCode != HttpURLConnection.HTTP_OK) {
	    // throw new AceQLException("HTTP_FAILURE" + " " + httpStatusCode
	    // + " " + httpStatusMessage, 0, httpStatusCode,
	    // httpStatusMessage);
	    // }

	    return in;

	} catch (Exception e) {
	    if (e instanceof AceQLException) {
		throw (AceQLException) e;
	    } else {
		throw new AceQLException(e.getMessage(), 0, e, null, httpStatusCode);
	    }
	}
    }

    /**
     * Formats & URL encode the the post data for POST.
     *
     * @param params the parameter names and values
     * @return the formated and URL encoded string for the POST.
     * @throws UnsupportedEncodingException
     */
    public static String getPostDataString(Map<String, String> requestParams) throws UnsupportedEncodingException {
	StringBuilder result = new StringBuilder();
	boolean first = true;

	for (Map.Entry<String, String> entry : requestParams.entrySet()) {

	    // trace(entry.getKey() + "/" + entry.getValue());

	    if (first)
		first = false;
	    else
		result.append("&");

	    if (entry.getValue() != null) {
		result.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
		result.append("=");
		result.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
	    }
	}

	return result.toString();
    }

    public int getHttpStatusCode() {
	return httpStatusCode;
    }

    /**
     * @return the httpStatusMessage
     */
    public String getHttpStatusMessage() {
	return httpStatusMessage;
    }

}
