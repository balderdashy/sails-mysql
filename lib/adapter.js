/*---------------------------------------------------------------
  :: sails-mysql
  -> adapter
---------------------------------------------------------------*/

// Dependencies
var async = require('async');
var _ = require('underscore');
_.str = require('underscore.string');
var mysql = require('mysql');
var Query = require('./query');
var utils = require('./utils');

// Get SQL waterline lib
var sql = require('./sql.js');

module.exports = (function() {

  // Keep track of all the dbs used by the app
  var dbs = {};

  var adapter = {

    // Whether this adapter is syncable (yes)
    syncable: true,

    defaults: {
      pool: true,
      connectionLimit: 10,
      waitForConnections: true
    },

    escape: function(val) {
      return mysql.escape(val);
    },

    escapeId: function(name) {
      return mysql.escapeId(name);
    },

    // Direct access to query
    query: function(collectionName, query, data, cb) {
      if (_.isFunction(data)) {
        cb = data;
        data = null;
      }

      spawnConnection(function(connection, cb) {

        // Run query
        if (data) connection.query(query, data, cb);
        else connection.query(query, cb);

      }, dbs[collectionName].config, cb);
    },

    registerCollection: function(collection, cb) {
      var def = _.clone(collection);
      var key = def.identity;

      if(dbs[key]) return cb();
      dbs[key.toString()] = def;

      // Create a Connection Pool if set
      if (def.config.pool) {
        adapter.pool = mysql.createPool(_.extend(marshalConfig(def.config), {
          connectionLimit: def.config.connectionLimit,
          waitForConnections: def.config.waitForConnections
        }));
      }

      return cb();
    },

    teardown: function(cb) {
      var my = this;

      if (adapter.defaults.pool) {
        // TODO: Drain pool
      }

      cb();
    },

    // Fetch the schema for a collection
    // (contains attributes and autoIncrement value)
    describe: function(collectionName, cb) {
      var self = this;

      spawnConnection(function __DESCRIBE__(connection, cb) {

        var tableName = mysql.escapeId(dbs[collectionName].identity);

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
            dbs[collectionName].schema = normalizedSchema;

            // TODO: check that what was returned actually matches the cache
            cb(null, normalizedSchema);
          });

        });
      }, dbs[collectionName].config, cb);
    },

    // Create a new collection
    define: function(collectionName, definition, cb) {
      spawnConnection(function __DEFINE__(connection, cb) {

        var def = dbs[collectionName];

        // Escape table name
        collectionName = mysql.escapeId(def.identity);

        // Iterate through each attribute, building a query string
        var schema = sql.schema(collectionName, definition);

        // Build query
        var query = 'CREATE TABLE ' + collectionName + ' (' + schema + ')';

        // Run query
        connection.query(query, function __DEFINE__(err, result) {
          if (err) return cb(err);
          cb(null, result);
        });

      }, dbs[collectionName].config, cb);
    },

    // Drop an existing collection
    drop: function(collectionName, cb) {
      spawnConnection(function __DROP__(connection, cb) {

        // Escape table name
        collectionName = mysql.escapeId(dbs[collectionName].identity);

        // Build query
        var query = 'DROP TABLE ' + collectionName;

        // Run query
        connection.query(query, function __DROP__(err, result) {
          if (err) {
            if (err.code === 'ER_BAD_TABLE_ERROR') {
              result = null;
            } else return cb(err);
          }
          cb(null, result);
        });
      }, dbs[collectionName].config, cb);
    },

    //
    addAttribute: function (collectionName, attrName, attrDef, cb) {
      spawnConnection(function(connection, cb) {
        var query = sql.addColumn(dbs[collectionName].identity, attrName, attrDef);

        // sails.log.verbose("ADD COLUMN QUERY ",query);

        // Run query
        connection.query(query, function(err, result) {
          if (err) return cb(err);

          // TODO: marshal response to waterline interface
          cb(err);
        });

      }, dbs[collectionName].config, cb);
    },

    //
    removeAttribute: function (collectionName, attrName, cb) {
      spawnConnection(function(connection, cb) {
        var query = sql.removeColumn(dbs[collectionName].identity, attrName);

        // sails.log.verbose("REMOVE COLUMN QUERY ",query);

        // Run query
        connection.query(query, function(err, result) {
          if (err) return cb(err);

          // TODO: marshal response to waterline interface
          cb(err);
        });

      }, dbs[collectionName].config, cb);
    },

    // No custom alter necessary-- alter can be performed by using the other methods (addAttribute, removeAttribute)
    // you probably want to use the default in waterline core since this can get complex
    // (that is unless you want some enhanced functionality-- then please be my guest!)

    // Create one or more new models in the collection
    create: function(collectionName, data, cb) {
      spawnConnection(function(connection, cb) {

        // Prepare values
        Object.keys(data).forEach(function(value) {
          data[value] = utils.prepareValue(data[value]);
        });

        var query = sql.insertQuery(dbs[collectionName].identity, data);

        // Run query
        connection.query(query, function(err, result) {

          if (err) return cb(err);

          // Build model to return
          var model = data;

          // If the insertId is non-zero, an autoIncrement column was incremented to this value.
          if (result.insertId && result.insertId !== 0) {
               model = _.extend({}, data, {

                // TODO: look up the autoIncrement attribute and increment that instead of assuming `id`
                id: result.insertId
              });
          }

          // Build a Query Object
          var _query = new Query(dbs[collectionName].definition);

          // Cast special values
          var values = _query.cast(model);

          cb(err, values);
        });
      }, dbs[collectionName].config, cb);
    },

    // Override of createEach to share a single connection
    // instead of using a separate connection for each request
    createEach: function (collectionName, valuesList, cb) {
      spawnConnection(function(connection, cb) {

        var records = [];

        async.eachSeries(valuesList, function (data, cb) {

          // Prepare values
          Object.keys(data).forEach(function(value) {
            data[value] = utils.prepareValue(data[value]);
          });

          // Run query
          var query = sql.insertQuery(dbs[collectionName].identity, data) + '; ';

          connection.query(query, function(err, results) {
            if (err) return cb(err);
            records.push(results.insertId);
            cb();
          });
        }, function(err) {
          if(err) return cb(err);

          // Build a Query to get newly inserted records
          var query = "SELECT * FROM " + dbs[collectionName].identity + " WHERE id IN (" + records + ");";

          // Run Query returing results
          connection.query(query, function(err, results) {
            if(err) return cb(err);

            var values = [];

            // Build a Query Object
            var _query = new Query(dbs[collectionName].definition);

            results.forEach(function(result) {
              values.push(_query.cast(result));
            });

            cb(null, values);
          });
        });

      }, dbs[collectionName].config, cb);
    },

    // Find one or more models from the collection
    // using where, limit, skip, and order
    // In where: handle `or`, `and`, and `like` queries
    find: function(collectionName, options, cb) {
      spawnConnection(function(connection, cb) {

        // Check if this is an aggregate query and that there is something to return
        if(options.groupBy || options.sum || options.average || options.min || options.max) {
          if(!options.sum && !options.average && !options.min && !options.max) {
            return cb(new Error('Cannot groupBy without a calculation'));
          }
        }

        // Build find query
        var query = sql.selectQuery(dbs[collectionName].identity, options);

        // Run query
        connection.query(query, function(err, result) {
          if(err) return cb(err);

          var values = [];

          // Build a Query Object
          var _query = new Query(dbs[collectionName].definition);

          result.forEach(function(item) {
            values.push(_query.cast(item));
          });

          cb(null, values);
        });
      }, dbs[collectionName].config, cb);
    },

    // Stream one or more models from the collection
    // using where, limit, skip, and order
    // In where: handle `or`, `and`, and `like` queries
    stream: function(collectionName, options, stream) {
      spawnConnection(function(connection, cb) {

        // Build find query
        var query = sql.selectQuery(dbs[collectionName].identity, options);

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
      }, dbs[collectionName].config);
    },

    // Update one or more models in the collection
    update: function(collectionName, options, values, cb) {
      spawnConnection(function(connection, cb) {

        // Escape table name
        var tableName = mysql.escapeId(dbs[collectionName].identity);

        // Find the record before updating it
        var criteria = sql.serializeOptions(dbs[collectionName].identity, options);

        var query = 'SELECT id FROM ' + tableName + ' ' + criteria;

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
          var query = 'UPDATE ' + tableName + ' SET ' + sql.updateCriteria(dbs[collectionName].identity, values) + ' ';

          query += sql.serializeOptions(dbs[collectionName].identity, options);

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
            adapter.find(collectionName, criteria, function(err, models) {
              if (err) return cb(err);

              var values = [];

              // Build a Query Object
              var _query = new Query(dbs[collectionName].definition);

              models.forEach(function(item) {
                values.push(_query.cast(item));
              });

              cb(err, values);
            });
          });

        });
      }, dbs[collectionName].config, cb);
    },

    // Delete one or more models from the collection
    destroy: function(collectionName, options, cb) {
      spawnConnection(function(connection, cb) {

        // Escape table name
        var tableName = mysql.escapeId(dbs[collectionName].identity);

        // Build query
        var query = 'DELETE FROM ' + tableName + ' ';

        query += sql.serializeOptions(dbs[collectionName].identity, options);

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
      }, dbs[collectionName].config, cb);
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
  function spawnConnection(logic, config, cb) {


    // Use a new connection each time
    if (!config.pool) {
      var connection = mysql.createConnection(marshalConfig(config));
      connection.connect(function(err) {
        afterwards(err, connection);
      });
    }

    // Use connection pooling
    else {
      adapter.pool.getConnection(afterwards);
    }

    // Run logic using connection, then release/close it

    function afterwards(err, connection) {
      if (err) {
        console.error("Error spawning mySQL connection:");
        console.error(err);
        if (connection) connection.end();
        return cb(err);
      }

      // console.log("Provisioned new connection.");
      // handleDisconnect(connection, config);

      logic(connection, function(err, result) {

        if (err) {
          console.error("Logic error in mySQL ORM.");
          console.error(err);
          connection.end();
          return cb && cb(err);
        }

        connection.end(function(err) {
          if (err) {
            console.error("MySQL connection killed with error:");
            console.error(err);
          }
          return cb && cb(err, result);
        });
      });
    }
  }

  function handleDisconnect(connection, config) {
    connection.on('error', function(err) {
      // if (!err.fatal) {
      //  return;
      // }

      if (!err || err.code !== 'PROTOCOL_CONNECTION_LOST') {
        // throw err;
      }

      console.error('Re-connecting lost connection: ' + err.stack);
      console.error(err);


      connection = mysql.createConnection(marshalConfig(config));
      connection.connect();
      // connection = mysql.createConnection(connection.config);
      // handleDisconnect(connection);
      // connection.connect();
    });
  }

  // Convert standard adapter config
  // into a custom configuration object for node-mysql
  function marshalConfig(config) {
    return {
      host: config.host,
      port: config.port || 3306,
      socketPath: config.socketPath || null,
      user: config.user,
      password: config.password,
      database: config.database
    };
  }

  return adapter;
})();
