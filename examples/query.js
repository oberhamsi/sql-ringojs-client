var sql = require('sql-ringojs-client');

sql.connect('jdbc:mysql://localhost/hello_world', 'com.mysql.jdbc.Driver', 'benchmarkdbuser', 'benchmarkdbpass');

// `close()` returns the connection to the pool.
// you must make sure that the connection is returned to the pool once
// and only once. 

try {
   var connection = sql.getConnection();
   var result = sql.query(connection, 'SELECT * FROM World WHERE id=2');
} catch (e) {
   connection.close();
   console.error(e);
   connection = null;
} finally {
   if (connection) {
      connection.close();
   }
}
  
