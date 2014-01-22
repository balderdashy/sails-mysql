/*---------------------------------------------------------------
  :: sails-mysql
  -> adapter
---------------------------------------------------------------*/

// Dependencies
var async = require('async'),
    _ = require('lodash'),
    mysql = require('mysql'),
    Query = require('./query'),
    utils = require('./utils'),
    sql = require('./sql.js');

module.exports = (function() {

  // Keep track of all the connections
  var connections = {};

  var adapter = {

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

    // Register A Connection
    registerConnection: function (connection, collections, cb) {

      if(!connection.identity) return cb(new Error('Connection is missing an identity'));
      if(connections[connection.identity]) return cb(new Error('Connection is already registered'));

      // Store the connection
      connections[connection.identity] = {
        config: connection,
        collections: collections,
        connection: {}
      };

      var activeConnection = connections[connection.identity];

      // Create a Connection Pool if set
      if (activeConnection.config.pool) {
        activeConnection.connection.pool = mysql.createPool(activeConnection.config);

        activeConnection.connection.releaseConnection = function(conn, cb) {
          conn.release();
          cb();
        };

        return cb();
      }

      // Define a releaseConnection function
      activeConnection.connection.releaseConnection = function(conn, cb) {
        conn.end(cb);
      };

      return cb();
    },


    // Direct access to query
    query: function(connectionName, collectionName, query, data, cb) {

      if (_.isFunction(data)) {
        cb = data;
        data = null;
      }

      spawnConnection(connectionName, function(connection, cb) {

        // Run query
        if (data) connection.query(query, data, cb);
        else connection.query(query, cb);

      }, cb);
    },

    teardown: function(connectionName, cb) {
      if(!connections[connectionName]) return cb();

      // Drain the connection pool if available
      if(connections[connectionName].connection.pool) {
        connections[connectionName].connection.pool.end();
      }

      delete connections[connectionName];
      cb();
    },

    // Fetch the schema for a collection
    // (contains attributes and autoIncrement value)
    describe: function(connectionName, collectionName, cb) {
      var self = this;

      spawnConnection(connectionName, function __DESCRIBE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = mysql.escapeId(collection.identity);

        var query = 'DESCRIBE ' + tableName;
        var pkQuery = "SHOW INDEX FROM " + tableName + ";";

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
      }, cb);
    },

    // Create a new collection
    define: function(connectionName, collectionName, definition, cb) {
      var self = this;

      spawnConnection(connectionName, function __DEFINE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = mysql.escapeId(collection.identity);

        // Iterate through each attribute, building a query string
        var schema = sql.schema(tableName, definition);

        // Build query
        var query = 'CREATE TABLE ' + tableName + ' (' + schema + ')';

        // Run query
        connection.query(query, function __DEFINE__(err, result) {
          if (err) return cb(err);
          self.describe(connectionName, collectionName, function(err) {
            cb(err, result);
          });
        });

      }, cb);
    },

    // Drop an existing collection
    drop: function(connectionName, collectionName, relations, cb) {

      if(typeof relations === 'function') {
        cb = relations;
        relations = [];
      }

      var connectionObject = connections[connectionName];

      spawnConnection(connectionName, function __DROP__(connection, cb) {

        // Drop any relations
        function dropTable(item, next) {

          var collection = connectionObject.collections[item];
          var tableName = mysql.escapeId(collection.identity);

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

      }, cb);
    },

    //
    addAttribute: function (connectionName, collectionName, attrName, attrDef, cb) {
      spawnConnection(connectionName, function __ADD_ATTRIBUTE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = mysql.escapeId(collection.identity);

        var query = sql.addColumn(tableName, attrName, attrDef);

        // Run query
        connection.query(query, function(err, result) {
          if (err) return cb(err);

          // TODO: marshal response to waterline interface
          cb(err);
        });

      }, cb);
    },

    //
    removeAttribute: function (connectionName, collectionName, attrName, cb) {
      spawnConnection(connectionName, function __REMOVE_ATTRIBUTE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = mysql.escapeId(collection.identity);

        var query = sql.removeColumn(tableName, attrName);

        // Run query
        connection.query(query, function(err, result) {
          if (err) return cb(err);

          // TODO: marshal response to waterline interface
          cb(err);
        });

      }, cb);
    },

    // No custom alter necessary-- alter can be performed by using the other methods (addAttribute, removeAttribute)
    // you probably want to use the default in waterline core since this can get complex
    // (that is unless you want some enhanced functionality-- then please be my guest!)

    // Create one or more new models in the collection
    create: function(connectionName, collectionName, data, cb) {
      spawnConnection(connectionName, function __CREATE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collection.identity;

        // Prepare values
        Object.keys(data).forEach(function(value) {
          data[value] = utils.prepareValue(data[value]);
        });

        var query = sql.insertQuery(tableName, data);

        // Run query
        connection.query(query, function(err, result) {
          if (err) return cb(err);

          // Build model to return
          var model = _.extend({}, data, {

            // TODO: look up the autoIncrement attribute and increment that instead of assuming `id`
            id: result.insertId
          });

          // Build a Query Object
          var _query = new Query(collection.definition);

          // Cast special values
          var values = _query.cast(model);

          cb(err, values);
        });
      }, cb);
    },

    // Override of createEach to share a single connection
    // instead of using a separate connection for each request
    createEach: function (connectionName, collectionName, valuesList, cb) {
      spawnConnection(connectionName, function __CREATE_EACH__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collection.identity;

        var records = [];

        async.eachSeries(valuesList, function (data, cb) {

          // Prepare values
          Object.keys(data).forEach(function(value) {
            data[value] = utils.prepareValue(data[value]);
          });

          // Run query
          var query = sql.insertQuery(tableName, data) + '; ';

          connection.query(query, function(err, results) {
            if (err) return cb(err);
            records.push(results.insertId);
            cb();
          });
        }, function(err) {
          if(err) return cb(err);

          // Build a Query to get newly inserted records
          var query = "SELECT * FROM " + tableName + " WHERE id IN (" + records + ");";

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

      }, cb);
    },

    // Find one or more models from the collection
    // using where, limit, skip, and order
    // In where: handle `or`, `and`, and `like` queries
    find: function(connectionName, collectionName, options, cb) {
      spawnConnection(connectionName, function __FIND__(connection, cb) {

        // Check if this is an aggregate query and that there is something to return
        if(options.groupBy || options.sum || options.average || options.min || options.max) {
          if(!options.sum && !options.average && !options.min && !options.max) {
            return cb(new Error('Cannot groupBy without a calculation'));
          }
        }

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collection.identity;

        // Build find query
        var schema = {};

        Object.keys(connectionObject.collections).forEach(function(coll) {
          schema[coll] = connectionObject.collections[coll].schema;
        });

        var query = sql.selectQuery(tableName, options, schema);

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
      }, cb);
    },

    // Stream one or more models from the collection
    // using where, limit, skip, and order
    // In where: handle `or`, `and`, and `like` queries
    stream: function(connectionName, collectionName, options, stream) {
      spawnConnection(connectionName, function __STREAM__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collection.identity;

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
      });
    },

    // Update one or more models in the collection
    update: function(connectionName, collectionName, options, values, cb) {
      spawnConnection(connectionName, function __UPDATE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collection.identity;

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

          results.forEach(function(result) {
            ids.push(result.id);
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
            if (err) return cb(err);

            var criteria;

            if(ids.length === 1) {
              criteria = { where: { id: ids[0] }, limit: 1 };
            } else {
              criteria = { where: { id: ids }};
            }

            // the update was successful, select the updated records
            adapter.find(connectionName, collectionName, criteria, function(err, models) {
              if (err) return cb(err);

              var values = [];

              // Build a Query Object
              var _query = new Query(collection.definition);

              models.forEach(function(item) {
                values.push(_query.cast(item));
              });

              cb(err, values);
            });
          });

        });
      }, cb);
    },

    // Delete one or more models from the collection
    destroy: function(connectionName, collectionName, options, cb) {
      spawnConnection(connectionName, function __DESTROY__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        var tableName = collection.identity;

        // Build query
        var query = 'DELETE FROM ' + mysql.escapeId(tableName) + ' ';

        query += sql.serializeOptions(tableName, options);

        // Run query
        connection.query(query, function(err, result) {

          var resultArray = [];

          // Normalize Result Array
          if(Array.isArray(result)) {
            result.forEach(function(value) {
              resultArray.push(value.insertId);
            });

            return cb(null, resultArray);
          }

          resultArray.push(result.insertId);
          cb(err, resultArray);
        });
      }, cb);
    },


    // Identity is here to facilitate unit testing
    // (this is optional and normally automatically populated based on filename)
    identity: 'sails-mysql'
  };



  //////////////                 //////////////////////////////////////////
  ////////////// Private Methods //////////////////////////////////////////
  //////////////                 //////////////////////////////////////////


  // Wrap a function in the logic necessary to provision a connection
  // (either grab a free connection from the pool or create a new one)
  // cb is optional (you might be streaming)
  function spawnConnection(connectionName, logic, cb) {

    var connectionObject = connections[connectionName];
    if(!connectionObject) return cb(new Error('Invalid Connection Name'));

    // If pooling is used, grab a connection from the pool and run the
    // logic for the query.
    if(connectionObject.connection.pool) {
      return connectionObject.connection.pool.getConnection(afterwards);
    }

    // Use a new connection each time
    var conn = mysql.createConnection(connectionObject.config);
    conn.connect(function(err) {
      afterwards(err, conn);
    });


    // Run logic using connection, then release/close it
    function afterwards(err, liveConnection) {
      if (err) {
        console.error("Error spawning mySQL connection:");
        console.error(err);
        connectionObject.connection.releaseConnection(liveConnection, function(){});
        return cb && cb(err);
      }

      logic(liveConnection, function(err, result) {
        if (err) {
          console.error("Logic error in mySQL ORM.");
          console.error(err);
          return connectionObject.connection.releaseConnection(liveConnection, function(){
            return cb && cb(err);
          });
        }

        connectionObject.connection.releaseConnection(liveConnection, function(err) {
          if (err) {
            console.error("MySQL connection killed with error:");
            console.error(err);
          }
          return cb && cb(err, result);
        });
      });
    }
  }

  return adapter;
})();
