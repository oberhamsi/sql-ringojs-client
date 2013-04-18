export('connect', 'getConnection', 'execute', 'query');

var {Types, DriverManager} = Packages.java.sql;

/**
 * Executes the given SQL statement. The result set is returned
 * as JavaScript Array containing a JavaScript Object for each result.
 *
 * eg.
 * query(conn, 'select * from users').map(function(x) { return x['FIRST_NAME']} );
 *
 * @param {String} sql an SQL query statement
 * @return {Array} an Array containing the result set
 *
 */
function query(connection, sql) {
//    var isLogSqlEnabled = (getProperty("logSQL", "false").toLowerCase() == "true");
//    var logTimeStart = isLogSqlEnabled ? java.lang.System.currentTimeMillis() : 0;
    connection.setReadOnly(true);
    var statement = connection.createStatement();
    var resultSet = statement.executeQuery(sql);
    var metaData = resultSet.getMetaData();
    var max = metaData.getColumnCount();
    var types = [];
    for (var i=1; i <= max; i++) {
        types[i] = metaData.getColumnType(i);
    }
    var result = [];
    while (resultSet.next()) {
        var row = {}
        for (var i=1; i<=max; i+=1) {
            switch (types[i]) {
                case Types.BIT:
                case Types.BOOLEAN:
                    row[metaData.getColumnLabel(i)] = resultSet.getBoolean(i);
                    break;
                case Types.TINYINT:
                case Types.BIGINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                    row[metaData.getColumnLabel(i)] = resultSet.getLong(i);
                    break;
                case Types.REAL:
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.DECIMAL:
                case Types.NUMERIC:
                    row[metaData.getColumnLabel(i)] = resultSet.getDouble(i);
                    break;
                case Types.VARBINARY:
                case Types.BINARY:
                case Types.LONGVARBINARY:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.CLOB:
                case Types.OTHER:
                    row[metaData.getColumnLabel(i)] = resultSet.getString(i);
                    break;
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    row[metaData.getColumnLabel(i)] = resultSet.getTimestamp(i);
                    break;
                case Types.NULL:
                    row[metaData.getColumnLabel(i)] = null;
                    break;
                default:
                    row[metaData.getColumnLabel(i)] = resultSet.getString(i);
                    break;
            }
        }
        result[result.length] = row;
    }
//    var logTimeStop = isLogSqlEnabled ? java.lang.System.currentTimeMillis() : 0;
//    if (isLogSqlEnabled) {
//        var tableName = metaData.getColumnCount() > 0 ? metaData.getTableName(1) : null;
//        app.getLogger("helma." + app.name + ".sql").info("SQL DIRECT_QUERY " + (tableName || "-") + " " + (logTimeStop - logTimeStart) + ": " + sql);
//    }
    try {
        statement.close();
        resultSet.close();
    } catch (error) {
        // ignore
    }
    return result;
};

/**
 * Executes the given SQL statement, which may be an INSERT, UPDATE,
 * or DELETE statement or an SQL statement that returns nothing,
 * such as an SQL data definition statement. The return value is an integer that
 * indicates the number of rows that were affected by the statement.
 * @param {String} sql an SQL statement
 * @return {int} either the row count for INSERT, UPDATE or
 * DELETE statements, or 0 for SQL statements that return nothing
 */
function execute(connection, sql) {
    //var isLogSqlEnabled = (getProperty("logSQL", "false").toLowerCase() == "true");
    //var logTimeStart = isLogSqlEnabled ? java.lang.System.currentTimeMillis() : 0;
    connection.setReadOnly(false);
    var statement = connection.createStatement();
    var result;
    try {
        result = statement.executeUpdate(sql);
    } finally {
        try {
            statement.close();
        } catch (error) {
            // ignore
        }
    }
//    var logTimeStop = isLogSqlEnabled ? java.lang.System.currentTimeMillis() : 0;
//    if (isLogSqlEnabled) {
//        app.getLogger("helma." + app.name + ".sql").info("SQL DIRECT_EXECUTE - " + (logTimeStop - logTimeStart) + ": " + sql);
//    }
    return result;
}


var pooledDatasource = null;

/**
 * Create a new Database instance using the given parameters.
 * <p>Some of the parameters support shortcuts for known database products.
 * The <code>url</code> parameter recognizes the values "mysql", "oracle" and
 * "postgresql". For those databases, it is also possible to pass just
 * <code>hostname</code> or <code>hostname:port</code> as <code>url</code>
 * parameters instead of the full JDBC URL.</p>
 * @param {String} driver the class name of the JDBC driver. As
 * shortcuts, the values "mysql", "oracle" and "postgresql" are
 * recognized.
 * @param {String} url the JDBC URL.
 * @param {String} name the name of the database to use
 * @param {String} user the the username
 * @param {String} password the password
 * @return {helma.Database} a helma.Database instance
 */
function connect(url, driver, user, password) {
    var fs = require('fs');
    // add all jar files to the classpath
    var dir = module.resolve("../../jars/");
    fs.list(dir).forEach(function(file) {
        if (fs.extension(file) === ".jar") {
            addToClasspath(fs.join(dir, file));
        }
    });
    var {GenericObjectPool} = Packages.org.apache.commons.pool.impl;
    importPackage(org.apache.commons.dbcp);
    pooledDatasource = module.singleton('ngv/pdss', function() {
        //Packages.java.lang.Class.forName(driver); //preload driver
        var connectionPool = new GenericObjectPool(null);
        var connectionFactory = new DriverManagerConnectionFactory(url, user, password);
        // http://commons.apache.org/proper/commons-dbcp/apidocs/org/apache/commons/dbcp/PoolableConnectionFactory.html
        var poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, connectionPool, null,null,false,true);
        return new PoolingDataSource(connectionPool);
    });
};

/**
 * make sure you `close()` it and don't share the connection with other threads
 */
function getConnection() {
    return pooledDatasource.getConnection();
};