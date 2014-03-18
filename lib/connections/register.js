/**
 * Module dependencies
 */

var mysql = require('mysql'),
    _releaseConnection = require('./release'),
    Errors = require('waterline-errors').adapter;


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
    if(!connection.identity) return cb(Errors.IdentityMissing);
    if(connections[connection.identity]) return cb(Errors.IdentityDuplicate);

    // Always ensure the schema key is set to something. This should be remapped in the
    // .describe() method later on.
    Object.keys(collections).forEach(function(coll) {
      collections[coll].schema = collections[coll].definition;
    });

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
