var sql = require('sql-ringojs-client');

var datasource = module.singleton('pooling-source', function() {
   return sql.connect('jdbc:mysql://localhost/hello_world', 'benchmarkdbuser', 'benchmarkdbpass');
});

// `close()` returns the connection to the pool.
// you must make sure that the connection is returned to the pool once
// and only once.

try {
   var connection = datasource.getConnection();
   var result = sql.query(connection, 'SELECT * FROM World WHERE id=2');
} catch (e) {
   console.error(e);
   connection.close();
   connection = null;
} finally {
   if (connection) {
      connection.close();
   }
}

console.dir(result);
