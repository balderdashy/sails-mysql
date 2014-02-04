/**
 * Module dependencies
 */

var mysql = require('mysql')
  , _releaseConnection = require('./release');




module.exports = {};


module.exports.configure = function ( connections ) {

  /**
   * Register a connection (and the collections assigned to it) with the MySQL adapter.
   * 
   * @param  {Connection} connection
   * @param  {Object} collections
   * @param  {Function} cb
   */

  return function registerConnection (connection, collections, cb) {

    // Validate arguments
    if(!connection.identity) return cb(new Error('Connection is missing an identity'));
    if(connections[connection.identity]) return cb(new Error('Connection (`'+connection.identity+'`) is already registered'));

    // Store the connection
    connections[connection.identity] = {
      config: connection,
      collections: collections,
      connection: {}
    };

    var activeConnection = connections[connection.identity];

    // Create a connection pool if configured to do so.
    // (and set up the necessary `releaseConnection` functionality to drain it.)
    if (activeConnection.config.pool) {
      activeConnection.connection.pool = mysql.createPool(activeConnection.config);
      activeConnection.connection.releaseConnection = _releaseConnection.poolfully;
    }
    // Otherwise, assign some default releaseConnection functionality.
    else {
      activeConnection.connection.releaseConnection = _releaseConnection.poollessly;
    }

    // Done!  The WLConnection (and all of it's collections) have been loaded.
    return cb();
  };

};