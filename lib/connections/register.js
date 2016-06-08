/**
 * Module dependencies
 */

var mysql = require('mysql');
var _releaseConnection = require('./release');
var Errors = require('waterline-errors').adapter;
var _ = require('lodash');
var utils = require('../utils');


module.exports = {};


module.exports.configure = function ( connections, sqlOptions ) {

  /**
   * Register a connection (and the collections assigned to it) with the MySQL adapter.
   *
   * @param  {Connection} connection
   * @param  {Object} collections
   * @param  {Function} cb
   */

  return function registerConnection (connection, collections, cb) {

    // Set the version of the API
    var version;
    if(connection.version) {
      version = connection.version;
    } else {
      version = 0;
    }

    // Validate arguments
    if(!connection.identity) {
      return cb(Errors.IdentityMissing);
    }

    if(connections[connection.identity]) {
      return cb(Errors.IdentityDuplicate);
    }

    // Build up a schema for this connection that can be used throughout the adapter
    var schema = {};

    _.each(_.keys(collections), function(collName) {
      var collection = collections[collName];
      if(!collection) {
        return;
      }

      // Normalize schema into a sane object and discard all the WL context
      var wlSchema = collection.waterline && collection.waterline.schema && collection.waterline.schema[collection.identity];
      var _schema = {};
      _schema.meta = collection.meta || {};
      _schema.tableName = wlSchema.tableName;
      _schema.connection = wlSchema.connection;

      // If a newer Adapter API is in use, the definition key is used to build
      // queries and the attributes property can be ignored.
      //
      // In older api versions SELECT statements were not normalized. Because of
      // this the attributes need to be stored that so SELECTS can be manually
      // normalized in the adapter before sending to the SQL builder.
      if(version > 0) {
        _schema.definition = collection.definition || {};
      } else {
        _schema.definition = collection.definition || {};
        _schema.attributes = wlSchema.attributes || {};
      }

      if(!_schema.tableName) {
        _schema.tableName = collName;
      }

      // If the connection names aren't the same we don't need it in the schema
      if(!_.includes(_schema.connection, connection.identity)) {
        return;
      }

      // If the tableName is different from the identity, store the tableName
      // in the schema.
      var schemaKey = collName;
      if(_schema.tableName !== collName) {
        schemaKey = _schema.tableName;
      }
      // Store the normalized schema
      schema[schemaKey] = _schema;
    });

    if('url' in connection) {
      utils.parseUrl(connection);
    }

    // Store the connection
    connections[connection.identity] = {
      config: connection,
      connection: {},
      schema: schema,
      version: version
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

    // if connection's wlNext.caseSensitive is set, pass it on as sqlOptions
    if (activeConnection.config.wlNext && activeConnection.config.wlNext.caseSensitive) {
      sqlOptions.caseSensitive = true;
      !sqlOptions.wlNext && (sqlOptions.wlNext = {});
      sqlOptions.wlNext.caseSensitive = true;
    }

    // Done!  The WLConnection (and all of it's collections) have been loaded.
    return cb();
  };


};
