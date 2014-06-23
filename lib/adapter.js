/*---------------------------------------------------------------
  :: sails-mysql
  -> adapter
---------------------------------------------------------------*/

// Dependencies
var async = require('async'),
    _ = require('lodash'),
    util = require('util'),
    mysql = require('mysql'),
    Query = require('./query'),
    utils = require('./utils'),
    _teardownConnection = require('./connections/teardown'),
    _spawnConnection = require('./connections/spawn'),
    _registerConnection = require('./connections/register'),
    sql = require('./sql.js'),
    Errors = require('waterline-errors').adapter;
var _runJoins = require('waterline-cursor');


var STRINGFILE = {
  noCallbackError: 'An error occurred in the MySQL adapter, but no callback was specified to the spawnConnection function to handle it.'
};

module.exports = (function() {

  // Keep track of all the connections
  var connections = {};

  var adapter = {

    //
    // TODO: make the exported thing an EventEmitter for when there's no callback.
    //
    emit: function (evName, data) {

      // temporary hack- should only be used for cases that would crash anyways
      // (see todo above- we still shouldn't throw, emit instead, hence this stub)
      if (evName === 'error') { throw data; }
    },

    // Which type of primary key is used by default
    pkFormat: 'integer',

    // Whether this adapter is syncable (yes)
    syncable: true,

    defaults: {
      pool: true,
      connectionLimit: 5,
      waitForConnections: true
    },

    escape: function(val) {
      return mysql.escape(val);
    },

    escapeId: function(name) {
      return mysql.escapeId(name);
    },


    registerConnection: _registerConnection.configure(connections),
    teardown: _teardownConnection.configure(connections),


    // Direct access to query
    query: function(connectionName, collectionName, query, data, cb, connection) {

      if (_.isFunction(data)) {
        cb = data;
        data = null;
      }

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __QUERY__, cb);
      } else {
        __QUERY__(connection, cb);
      }

      function __QUERY__(connection, cb) {

        // Run query
        if (data) connection.query(query, data, cb);
        else connection.query(query, cb);

      }
    },


    // Fetch the schema for a collection
    // (contains attributes and autoIncrement value)
    describe: function(connectionName, collectionName, cb, connection) {

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __DESCRIBE__, cb);
      } else {
        __DESCRIBE__(connection, cb);
      }

      function __DESCRIBE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        if (!collection) {
          return cb(util.format('Unknown collection `%s` in connection `%s`', collectionName, connectionName));
        }
        var tableName = mysql.escapeId(collectionName);

        var query = 'DESCRIBE ' + tableName;
        var pkQuery = 'SHOW INDEX FROM ' + tableName + ';';

        connection.query(query, function __DESCRIBE__(err, schema) {
          if (err) {
            if (err.code === 'ER_NO_SUCH_TABLE') {
              return cb();
            } else return cb(err);
          }

          connection.query(pkQuery, function(err, pkResult) {
            if(err) return cb(err);

            // Loop through Schema and attach extra attributes
            schema.forEach(function(attr) {

              // Set Primary Key Attribute
              if(attr.Key === 'PRI') {
                attr.primaryKey = true;

                // If also an integer set auto increment attribute
                if(attr.Type === 'int(11)') {
                  attr.autoIncrement = true;
                }
              }

              // Set Unique Attribute
              if(attr.Key === 'UNI') {
                attr.unique = true;
              }
            });

            // Loop Through Indexes and Add Properties
            pkResult.forEach(function(result) {
              schema.forEach(function(attr) {
                if(attr.Field !== result.Column_name) return;
                attr.indexed = true;
              });
            });

            // Convert mysql format to standard javascript object
            var normalizedSchema = sql.normalizeSchema(schema);

            // Set Internal Schema Mapping
            collection.schema = normalizedSchema;

            // TODO: check that what was returned actually matches the cache
            cb(null, normalizedSchema);
          });

        });
      }
    },

    // Create a new collection
    define: function(connectionName, collectionName, definition, cb, connection) {
      var self = this;

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __DEFINE__, cb);
      } else {
        __DEFINE__(connection, cb);
      }

      function __DEFINE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        if (!collection) {
          return cb(util.format('Unknown collection `%s` in connection `%s`', collectionName, connectionName));
        }
        var tableName = mysql.escapeId(collectionName);

        // Iterate through each attribute, building a query string
        var schema = sql.schema(tableName, definition);

        // Build query
        var query = 'CREATE TABLE ' + tableName + ' (' + schema + ')';

        if(connectionObject.config.charset) {
          query += ' DEFAULT CHARSET ' + connectionObject.config.charset;
        }

        if(connectionObject.config.collation) {
          if(!connectionObject.config.charset) query += ' DEFAULT ';
          query += ' COLLATE ' + connectionObject.config.collation;
        }


        // Run query
        connection.query(query, function __DEFINE__(err, result) {
          if (err) return cb(err);

          //
          // TODO:
          // Determine if this can safely be changed to the `adapter` closure var
          // (i.e. this is the last remaining usage of the "this" context in the MySQLAdapter)
          //

          self.describe(connectionName, collectionName, function(err) {
            cb(err, result);
          });
        });

      }
    },

    // Drop an existing collection
    drop: function(connectionName, collectionName, relations, cb, connection) {

      if(typeof relations === 'function') {
        cb = relations;
        relations = [];
      }

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __DROP__, cb);
      } else {
        __DROP__(connection, cb);
      }

      function __DROP__(connection, cb) {

        var connectionObject = connections[connectionName];


        // Drop any relations
        function dropTable(item, next) {

          var collection = connectionObject.collections[item];
          var tableName = mysql.escapeId(collectionName);

          // Build query
          var query = 'DROP TABLE ' + tableName;

          // Run query
          connection.query(query, function __DROP__(err, result) {
            if (err) {
              if (err.code !== 'ER_BAD_TABLE_ERROR' && err.code !== 'ER_NO_SUCH_TABLE') return next(err);
              result = null;
            }

            next(null, result);
          });
        }

        async.eachSeries(relations, dropTable, function(err) {
          if(err) return cb(err);
          dropTable(collectionName, cb);
        });

      }
    },

    //
    addAttribute: function (connectionName, collectionName, attrName, attrDef, cb, connection) {

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __ADD_ATTRIBUTE__, cb);
      } else {
        __ADD_ATTRIBUTE__(connection, cb);
      }

      function __ADD_ATTRIBUTE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collectionName;

        var query = sql.addColumn(tableName, attrName, attrDef);

        // Run query
        connection.query(query, function(err, result) {
          if (err) return cb(err);

          // TODO: marshal response to waterline interface
          cb(err);
        });

      }
    },

    //
    removeAttribute: function (connectionName, collectionName, attrName, cb, connection) {

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __REMOVE_ATTRIBUTE__, cb);
      } else {
        __REMOVE_ATTRIBUTE__(connection, cb);
      }

      function __REMOVE_ATTRIBUTE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collectionName;

        var query = sql.removeColumn(tableName, attrName);

        // Run query
        connection.query(query, function(err, result) {
          if (err) return cb(err);

          // TODO: marshal response to waterline interface
          cb(err);
        });

      }
    },

    // No custom alter necessary-- alter can be performed by using the other methods (addAttribute, removeAttribute)
    // you probably want to use the default in waterline core since this can get complex
    // (that is unless you want some enhanced functionality-- then please be my guest!)

    // Create one or more new models in the collection
    create: function(connectionName, collectionName, data, cb, connection) {

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __CREATE__, cb);
      } else {
        __CREATE__(connection, cb);
      }

      function __CREATE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collectionName;

        // Prepare values
        Object.keys(data).forEach(function(value) {
          data[value] = utils.prepareValue(data[value]);
        });

        var query = sql.insertQuery(tableName, data);

        // Run query
        connection.query(query, function(err, result) {
          if (err) return cb( handleQueryError(err) );

          // Build model to return
          var autoInc = null;

          Object.keys(collection.definition).forEach(function(key) {
            if(!collection.definition[key].hasOwnProperty('autoIncrement')) return;
            autoInc = key;
          });

          var autoIncData = {};

          if (autoInc) {

            autoIncData[autoInc] = result.insertId;

          }

          var model = _.extend({}, data, autoIncData);

          // Build a Query Object
          var _query = new Query(collection.definition);

          // Cast special values
          var values = _query.cast(model);

          cb(err, values);
        });
      }
    },

    // Override of createEach to share a single connection
    // instead of using a separate connection for each request
    createEach: function (connectionName, collectionName, valuesList, cb, connection) {

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __CREATE_EACH__, cb);
      } else {
        __CREATE_EACH__(connection, cb);
      }


      function __CREATE_EACH__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collectionName;

        var records = [];

        async.eachSeries(valuesList, function (data, cb) {

          // Prepare values
          Object.keys(data).forEach(function(value) {
            data[value] = utils.prepareValue(data[value]);
          });

          // Run query
          var query = sql.insertQuery(tableName, data) + '; ';

          connection.query(query, function(err, results) {
            if (err) return cb( handleQueryError(err) );
            records.push(results.insertId);
            cb();
          });
        }, function(err) {
          if(err) return cb(err);

          var pk = 'id';

          Object.keys(collection.definition).forEach(function(key) {
            if(!collection.definition[key].hasOwnProperty('primaryKey')) return;
            pk = key;
          });

          // Build a Query to get newly inserted records
          var query = 'SELECT * FROM ' + mysql.escapeId(tableName) + ' WHERE ' + mysql.escapeId(pk) + ' IN (' + records + ');';

          // Run Query returing results
          connection.query(query, function(err, results) {
            if(err) return cb(err);

            var values = [];

            // Build a Query Object
            var _query = new Query(collection.definition);

            results.forEach(function(result) {
              values.push(_query.cast(result));
            });

            cb(null, values);
          });
        });

      }
    },

    /**
     * [join description]
     * @param  {[type]} conn     [description]
     * @param  {[type]} coll     [description]
     * @param  {[type]} criteria [description]
     * @param  {[type]} cb      [description]
     * @return {[type]}          [description]
     */
    join: function (conn, coll, criteria, cb, liveConnection) {

      (function (_onwards) {
        if (_.isUndefined(liveConnection)) {
          return spawnConnection(connectionName, _onwards, cb);
        }
        else _onwards(liveConnection, cb);
      })(function _join (liveConnection, _done) {

        // Fetch parent results
        var parentIdentity = coll;
        adapter.find(conn, coll, {
          where: criteria.where,
          limit: criteria.limit,
          skip: criteria.skip,
          sort: criteria.sort
        }, function _gotParentResults (err, parentResults){
          if (err) return _done(err);

          // Populate associated records for each parent result
          // (or do them all at once as an optimization, if possible)
          _runJoins({

            parentResults: parentResults,

            instructions: criteria.joins,

            /**
             * Find some records directly (using only this adapter)
             * from the specified collection.
             * 
             * @param  {String}   collectionIdentity
             * @param  {Object}   criteria
             * @param  {Function} _cb
             */
            $find: function (collectionIdentity, criteria, _cb) {
              return adapter.find(conn, collectionIdentity, criteria, _cb, liveConnection);
            },

            /**
             * Look up the name of the primary key field
             * for the collection with the specified identity.
             * 
             * @param  {String}   collectionIdentity
             * @return {String}
             */
            $getPK: function (collectionIdentity) {
              if (!collectionIdentity) return;

              return _getPK(conn, collectionIdentity);
            }
          }, _done);

        }, liveConnection);// <find parent results>
      });
    },

    // join: function(connectionName, collectionName, options, cb) {
    //   adapter.find(connectionName, collectionName, options, cb);
    // },


    // Find one or more models from the collection
    // using where, limit, skip, and order
    // In where: handle `or`, `and`, and `like` queries
    find: function(connectionName, collectionName, options, cb, connection) {

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __FIND__, cb);
      } else {
        __FIND__(connection, cb);
      }

      function __FIND__(connection, cb) {

        // Check if this is an aggregate query and that there is something to return
        if(options.groupBy || options.sum || options.average || options.min || options.max) {
          if(!options.sum && !options.average && !options.min && !options.max) {
            return cb(Errors.InvalidGroupBy);
          }
        }

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collectionName;

        // Build a copy of the schema to send w/ the query
        var localSchema = _.reduce(connectionObject.collections, function (localSchema, collection, cid) {
          localSchema[cid] = collection.schema;
          return localSchema;
        }, {});

        // Build find query
        var query = sql.selectQuery(tableName, options, localSchema);

        // Run query
        connection.query(query, function(err, result) {
          if(err) return cb(err);

          var values = [];

          // Build a Query Object
          var _query = new Query(collection.definition);

          result.forEach(function(item) {
            values.push(_query.cast(item));
          });

          // If a join was used the values should be grouped to normalize the
          // result into objects
          var _values = options.joins ? utils.group(values) : values;

          cb(null, _values);
        });

      }
    },

      // Count one model from the collection
      // using where, limit, skip, and order
      // In where: handle `or`, `and`, and `like` queries
    count: function(connectionName, collectionName, options, cb, connection) {

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __COUNT__, cb);
      } else {
        __COUNT__(connection, cb);
      }

      function __COUNT__(connection, cb) {

        // Check if this is an aggregate query and that there is something to return
        if(options.groupBy || options.sum || options.average || options.min || options.max) {
          if(!options.sum && !options.average && !options.min && !options.max) {
            return cb(Errors.InvalidGroupBy);
          }
        }

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collectionName;

        // Build a copy of the schema to send w/ the query
        var localSchema = _.reduce(connectionObject.collections, function (localSchema, collection, cid) {
          localSchema[cid] = collection.schema;
          return localSchema;
        }, {});

        // Build find query
        var query = sql.countQuery(tableName, options, localSchema);

        // Run query
        connection.query(query, function(err, result) {
          if(err) return cb(err);
          // Return the count from the simplified query
          cb(null, result[0].count);
        });
      }
    },

    // Stream one or more models from the collection
    // using where, limit, skip, and order
    // In where: handle `or`, `and`, and `like` queries
    stream: function(connectionName, collectionName, options, stream, connection) {

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __STREAM__);
      } else {
        __STREAM__(connection);
      }

      function __STREAM__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collectionName;

        // Build find query
        var query = sql.selectQuery(tableName, options);

        // Run query
        var dbStream = connection.query(query);

        // Handle error, an 'end' event will be emitted after this as well
        dbStream.on('error', function(err) {
          stream.end(err); // End stream
          cb(err); // Close connection
        });

        // the field packets for the rows to follow
        dbStream.on('fields', function(fields) {});

        // Pausing the connnection is useful if your processing involves I/O
        dbStream.on('result', function(row) {
          connection.pause();
          stream.write(row, function() {
            connection.resume();
          });
        });

        // all rows have been received
        dbStream.on('end', function() {
          stream.end(); // End stream
          cb(); // Close connection
        });
      }
    },

    // Update one or more models in the collection
    update: function(connectionName, collectionName, options, values, cb, connection) {

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __UPDATE__, cb);
      } else {
        __UPDATE__(connection, cb);
      }

      function __UPDATE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collectionName;

        // Find the record before updating it
        var criteria = sql.serializeOptions(tableName, options);

        var schema = {};

        Object.keys(connectionObject.collections).forEach(function(coll) {
          schema[coll] = connectionObject.collections[coll].schema;
        });

        var query = sql.selectQuery(tableName, options, schema);

        connection.query(query, function(err, results) {
          if(err) return cb(err);

          var ids = [];
          
          var pk = 'id';
          Object.keys(collection.definition).forEach(function(key) {
            if(!collection.definition[key].hasOwnProperty('primaryKey')) return;
            pk = key;
          });

          results.forEach(function(result) {

            // update statement will affect 0 rows
            if (results.length === 0) {
              return cb(null, []);
            }

            ids.push(result[pk]);
          });

          // Prepare values
          Object.keys(values).forEach(function(value) {
            values[value] = utils.prepareValue(values[value]);
          });

          // Build query
          var query = 'UPDATE ' + mysql.escapeId(tableName) + ' SET ' + sql.updateCriteria(tableName, values) + ' ';

          query += sql.serializeOptions(tableName, options);

          // Run query
          connection.query(query, function(err, result) {
            if (err) return cb( handleQueryError(err) );

            var criteria;
            if(ids.length === 1) {
              criteria = { where: {}, limit: 1 };
              criteria.where[pk] = ids[0];
            } else {
              criteria = { where: {} };
              criteria.where[pk] = ids;
            }


            // the update was successful, select the updated records


            // Build a copy of the schema to send w/ the query
            var localSchema = _.reduce(connectionObject.collections, function (localSchema, collection, cid) {
              localSchema[cid] = collection.schema;
              return localSchema;
            }, {});

            // Build find query
            var query = sql.selectQuery(tableName, options, localSchema);

            // Run query
            connection.query(query, function(err, result) {
              if(err) return cb(err);

              var values = [];

              // Build a Query Object
              var _query = new Query(collection.definition);

              result.forEach(function(item) {
                values.push(_query.cast(item));
              });

              // If a join was used the values should be grouped to normalize the
              // result into objects
              var _values = options.joins ? utils.group(values) : values;

              cb(null, _values);
            });
          });

        });
      }
    },

    // Delete one or more models from the collection
    destroy: function(connectionName, collectionName, options, cb, connection) {

      if(_.isUndefined(connection)) {
        return spawnConnection(connectionName, __DESTROY__, cb);
      } else {
        __DESTROY__(connection, cb);
      }

      function __DESTROY__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collectionName;

        // Build query
        var query = 'DELETE FROM ' + mysql.escapeId(tableName) + ' ';

        query += sql.serializeOptions(tableName, options);

        async.auto({

          findRecords: function(next) {
            adapter.find(connectionName, collectionName, options, next, connection);
          },

          destroyRecords: ['findRecords', function(next) {
            connection.query(query, next);
          }]
        },
        function(err, results) {
          if(err) return cb(err);
          cb(null, results.findRecords);
        });

      }
    },


    // Identity is here to facilitate unit testing
    // (this is optional and normally automatically populated based on filename)
    identity: 'sails-mysql'
  };



  return adapter;



  /**
   * Wrap a function in the logic necessary to provision a connection.
   * (either grab a free connection from the pool or create a new one)
   *
   * cb is optional (you might be streaming), but... come to think of it...
   * TODO:
   * if streaming, pass in the stream instead of the callback--
   * then any relevant `error` events can be emitted on the stream.
   *
   * @param  {[type]}   connectionName
   * @param  {Function} fn
   * @param  {[type]}   cb
   */
  function spawnConnection(connectionName, fn, cb) {
    _spawnConnection(
      getConnectionObject(connectionName),
      fn,
      wrapCallback(cb)
    );
  }




  ////// NOTE /////////////////////////////////////////////////////////////
  //
  // both of these things should be done in WL core, imo:
  //
  // i.e.
  // getConnectionObject(connectionName)
  // wrapCallback(cb)
  //
  /////////////////////////////////////////////////////////////////////////



  /**
   * wrapCallback
   *
   * cb is optional (you might be streaming), but... come to think of it...
   * TODO:
   * if streaming, pass in the stream instead of the callback--
   * then any relevant `error` events can be emitted on the stream.
   *
   * @param  {Function} cb [description]
   * @return {[type]}      [description]
   */
  function wrapCallback (cb) {

    // Handle missing callback:
    if (!cb) {
      // Emit errors on adapter itself when no callback is present.
      cb = function (err) {
        try {
          adapter.emit(STRINGFILE.noCallbackError+'\n'+err.toString());
        }
        catch (e) { adapter.emit(err); }
      };
    }
    return cb;
  }


  /**
   * Lookup the primary key for the given collection
   * @param  {[type]} collectionIdentity [description]
   * @return {[type]}                    [description]
   * @api private
   */
  function _getPK (connectionIdentity, collectionIdentity) {

    var collectionDefinition;
    try {
      collectionDefinition = connections[connectionIdentity].collections[collectionIdentity].definition;

      return _.find(collectionDefinition, function _findPK (key){
        var attrDef = collectionDefinition[key];
        if( attrDef && attrDef.primaryKey ) return key;
        else return false;
      }) || 'id';
    }
    catch (e) {
      throw new Error('Unable to determine primary key for collection `'+collectionIdentity+'` because '+
        'an error was encountered acquiring the collection definition:\n'+ require('util').inspect(e,false,null));
    }
  }


  /**
   *
   * @param  {String} connectionName
   * @return {Object} connectionObject
   */
  function getConnectionObject ( connectionName ) {

    var connectionObject = connections[connectionName];
    if(!connectionObject) {

      // this should never happen unless the adapter is being called directly
      // (i.e. outside of a CONNection OR a COLLection.)
      adapter.emit('error', Errors.InvalidConnection);
    }
    return connectionObject;
  }

  /**
   *
   * @param  {[type]} err [description]
   * @return {[type]}     [description]
   * @api private
   */
  function handleQueryError (err) {

    var formattedErr;

    // Check for uniqueness constraint violations:
    if (err.code === 'ER_DUP_ENTRY') {

      // Manually parse the MySQL error response and extract the relevant bits,
      // then build the formatted properties that will be passed directly to
      // WLValidationError in Waterline core.
      var matches = err.message.match(/Duplicate entry '(.*)' for key '(.*?)'$/);
      if (matches && matches.length) {
        formattedErr = {};
        formattedErr.code = 'E_UNIQUE';
        formattedErr.invalidAttributes = {};
        formattedErr.invalidAttributes[matches[2]] = [{
          value: matches[1],
          rule: 'unique'
        }];
      }
    }

    return formattedErr || err;
  }

})();

