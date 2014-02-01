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
   * @param  {Function} cb__registerConnection
   */

  return function registerConnection (connection, collections, cb__registerConnection) {

    // Validate arguments
    if(!connection.identity) return cb__registerConnection(new Error('Connection is missing an identity'));
    if(connections[connection.identity]) return cb__registerConnection(new Error('Connection is already registered'));

    // Store the connection
    connections[connection.identity] = {
      config: connection,
      collections: collections,
      connection: {}
    };

    var activeConnection = connections[connection.identity];

    // Create a connection pool if configured to do so.
    if (activeConnection.config.pool) {
      activeConnection.connection.pool = mysql.createPool(activeConnection.config);
      activeConnection.connection.releaseConnection = _releaseConnection.poolfully;
    }
    else {
      activeConnection.connection.releaseConnection = _releaseConnection.poollessly;
    }

    // Done.
    return cb__registerConnection();
  };

};