/**
 * Module dependencies
 */

var mysql = require('mysql');
var _releaseConnection = require('./release');
var Errors = require('waterline-errors').adapter;
var _ = require('lodash');
var utils = require('../utils');


module.exports = {};

function inheritConfigProperties (source, dest, properties) {
  properties.forEach(function (name) {
    if (_.isUndefined(dest[name])) {
      dest[name] = source[name];
    }
  });
}

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

    // Build up a schema for this connection that can be used throughout the adapter
    var schema = {};

    _.each(_.keys(collections), function(coll) {
      var collection = collections[coll];
      if(!collection) return;

      var _schema = collection.waterline && collection.waterline.schema && collection.waterline.schema[collection.identity];
      if(!_schema) return;

      // Set defaults to ensure values are set
      if(!_schema.attributes) _schema.attributes = {};
      if(!_schema.tableName) _schema.tableName = coll;

      // If the connection names are't the same we don't need it in the schema
      if(!_.includes(collections[coll].connection, connection.identity)) {
        return;
      }

      // If the tableName is different from the identity, store the tableName in the schema
      var schemaKey = coll;
      if(_schema.tableName != coll) {
        schemaKey = _schema.tableName;
      }

      schema[schemaKey] = _schema;
    });

    if('url' in connection) {
      utils.parseUrl(connection);
    }

    // Store the connection
    connections[connection.identity] = {
      config: connection,
      collections: collections,
      connection: {},
      schema: schema
    };

    var activeConnection = connections[connection.identity];

    // Create a connection pool cluster if configured to do so.
    // (and set up the necessary `releaseConnection` functionality to drain it.)
    if (activeConnection.config.replication) {
      activeConnection.connection.poolCluster = mysql.createPoolCluster({
        canRetry: activeConnection.config.replication.canRetry || true,
        defaultSelector: activeConnection.config.replication.defaultSelector || 'RR'
      });

      activeConnection.config.replication.read = activeConnection.config.replication.read || [];
      activeConnection.config.replication.write = activeConnection.config.replication.write || [];
      activeConnection.config.replication.readwrite = activeConnection.config.replication.readwrite || [];

      activeConnection.config.replication.read = activeConnection.config.replication.read.concat(activeConnection.config.replication.readwrite);
      activeConnection.config.replication.write = activeConnection.config.replication.write.concat(activeConnection.config.replication.readwrite);

      activeConnection.config.replication.read.forEach(function (config, index) {
        inheritConfigProperties(activeConnection.config, config, ['user', 'password', 'database']);
        activeConnection.connection.poolCluster.add('READ' + index, config);
      });

      activeConnection.config.replication.write.forEach(function (config, index) {
        inheritConfigProperties(activeConnection.config, config, ['user', 'password', 'database']);
        activeConnection.connection.poolCluster.add('WRITE' + index, config);
      });

      activeConnection.connection.releaseConnection = _releaseConnection.poolfully;
    // Create a connection pool if configured to do so.
    // (and set up the necessary `releaseConnection` functionality to drain it.)
    } else if (activeConnection.config.pool) {
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
