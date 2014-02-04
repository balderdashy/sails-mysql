/**
 * Module dependencies
 */



module.exports = {};


module.exports.configure = function ( connections ) {

 /**
   * Teardown a MySQL connection.
   * (if the Waterline "connection" is using a pool, also `.end()` it.)
   *
   * @param  {String}   connectionName  [name of the Waterline "connection"]
   * @param  {Function} cb
   */
  return function teardown (connectionName, cb) {
    
    // Drain the MySQL connection pool for this Waterline Connection
    // (if it's in use.)
    if ( connections[connectionName].connection.pool ) {
      // console.log('Ending pool for ' + connectionName);
      connections[connectionName].connection.pool.end();
    }

    // Make sure memory is freed by removing this stuff from our
    // global set of WL Connections.
    delete connections[connectionName];
    
    cb();
  };

};