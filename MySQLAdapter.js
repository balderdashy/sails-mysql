/*---------------------------------------------------------------
	:: sails-mysql
	-> adapter
---------------------------------------------------------------*/

// Dependencies
var async = require('async');
var _ = require('underscore');
_.str = require('underscore.string');
var mysql = require('mysql');

// Get SQL waterline lib
var sql = require('./sql.js');

module.exports = (function() {

	// Keep track of all the dbs used by the app
	var dbs = {};

	var adapter = {

		// Whether this adapter is syncable (yes)
		syncable: true,

		// Enable dev-only commit log for now (in the future, native transaction support will be added)
		commitLog: {
			identity: '__default_waterline_mysql_transaction',
			adapter: 'sails-dirty',
			inMemory: true
		},

		defaults: {

			// Pooling doesn't work yet, so it's off by default
			pool: false
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

			}, dbs[collectionName], cb);
		},

		// Initialize the underlying data model
		registerCollection: function(collection, cb) {
			var self = this;

			// If the configuration in this collection corresponds 
			// with a known database, reuse it the connection(s) to that db
			dbs[collection.identity] = _.find(dbs, function(db) {
				return collection.host === db.host && collection.database === db.database;
			});

			// Otherwise initialize for the first time
			if (!dbs[collection.identity]) {

				dbs[collection.identity] = marshalConfig(collection);

				// Create the connection pool (if configured to do so)
				// TODO: make this actually work
				if (collection.pool) {
					adapter.pool = mysql.createPool(marshalConfig(collection));

					// Always make sure to keep a single connection tethered
					// to prevent shutdowns due to not having any live connections 
					// (hopefully this will be resolved in a subsequent release of node-mysql)
					adapter.pool.getConnection(function(err, connection) {
						if (err) return cb(err);
						else adapter.tether = connection;

						cb();
					});
				} else return cb();
			} else return cb();

		},

		teardown: function(cb) {
			var my = this;

			if (adapter.defaults.pool) {
				// TODO: Drain pool
			}

			cb && cb();
		},


		// Fetch the schema for a collection
		// (contains attributes and autoIncrement value)
		describe: function(collectionName, cb) {
			var self = this;
			spawnConnection(function __DESCRIBE__(connection, cb) {
				var tableName = mysql.escapeId(collectionName);
				var query = 'DESCRIBE ' + tableName;
				connection.query(query, function __DESCRIBE__(err, schema) {
					if (err) {
						if (err.code === 'ER_NO_SUCH_TABLE') {
							return cb();
						} else return cb(err);
					}

					// Convert mysql format to standard javascript object
					schema = sql.normalizeSchema(schema);

					// TODO: check that what was returned actually matches the cache
					cb(null, schema);
				});
			}, dbs[collectionName], cb);
		},

		// Create a new collection
		define: function(collectionName, definition, cb) {
			spawnConnection(function __DEFINE__(connection, cb) {

				// Escape table name
				collectionName = mysql.escapeId(collectionName);

				// Iterate through each attribute, building a query string
				var $schema = sql.schema(collectionName, definition.attributes);

				// Build query
				var query = 'CREATE TABLE ' + collectionName + ' (' + $schema + ')';

				// Run query
				connection.query(query, function __DEFINE__(err, result) {
					if (err) return cb(err);
					cb(null, result);
				});
			}, dbs[collectionName], cb);
		},

		// Drop an existing collection
		drop: function(collectionName, cb) {
			spawnConnection(function __DROP__(connection, cb) {

				// Escape table name
				collectionName = mysql.escapeId(collectionName);

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
			}, dbs[collectionName], cb);
		},

		//
		addAttribute: function (collectionName, attrName, attrDef, cb) {
			spawnConnection(function(connection, cb) {
				var query = sql.addColumn(collectionName, attrName, attrDef);

				sails.log.verbose("ADD COLUMN QUERY ",query);
				
				// Run query
				connection.query(query, function(err, result) {
					if (err) return cb(err);

					// TODO: marshal response to waterline interface
					cb(err);
				});

			}, dbs[collectionName], cb);
		},

		//
		removeAttribute: function (collectionName, attrName, cb) {
			spawnConnection(function(connection, cb) {
				var query = sql.removeColumn(collectionName, attrName);

				sails.log.verbose("REMOVE COLUMN QUERY ",query);

				// Run query
				connection.query(query, function(err, result) {
					if (err) return cb(err);

					// TODO: marshal response to waterline interface
					cb(err);
				});

			}, dbs[collectionName], cb);
		},

		// No custom alter necessary-- alter can be performed by using the other methods (addAttribute, removeAttribute)
		// you probably want to use the default in waterline core since this can get complex
		// (that is unless you want some enhanced functionality-- then please be my guest!)

		// Create one or more new models in the collection
		create: function(collectionName, data, cb) {
			spawnConnection(function(connection, cb) {

				var query = sql.insertQuery(collectionName, data);

				// Run query
				connection.query(query, function(err, result) {

					if (err) return cb(err);

					// Build model to return
					var model = _.extend({}, data, {

						// TODO: look up the autoIncrement attribute and increment that instead of assuming `id`
						id: result.insertId
					});

					cb(err, model);
				});
			}, dbs[collectionName], cb);
		},

		// Override of createEach to share a single connection
		// instead of using a separate connection for each request
		createEach: function (collectionName, valuesList, cb) {
			spawnConnection(function(connection, cb) {
				async.forEach(valuesList, function (data, cb) {

					// Run query
					var query = sql.insertQuery(collectionName, data) + '; ';
					connection.query(query, function(err, results) {
						if (err) return cb(err);
						cb(err, results);
					});
				}, cb);


				////////////////////////////////////////////////////////////////////////////////////
				// node-mysql does not support multiple statements in a single query
				// There are ways to fix this, but for now, we're using the more naive solution
				//
				// Here's what doing it w/ multiple statements/single query would look like, roughly:
				//
				////////////////////////////////////////////////////////////////////////////////////
				// // Build giant query
				// var query = '';
				// _.each(valuesList, function (data) {
				// 	query += sql.insertQuery(collectionName, data) + '; ';
				// });

				// // Run query
				// connection.query(query, function(err, results) {
				// 	if (err) return cb(err);
				// 	cb(err, results);
				// });

			}, dbs[collectionName], cb);
		},

		// Find one or more models from the collection
		// using where, limit, skip, and order
		// In where: handle `or`, `and`, and `like` queries
		find: function(collectionName, options, cb) {
			spawnConnection(function(connection, cb) {

				// Build find query
				var query = sql.selectQuery(collectionName, options);

				// Run query
				connection.query(query, function(err, result) {
					cb(err, result);
				});
			}, dbs[collectionName], cb);
		},

		// Stream one or more models from the collection
		// using where, limit, skip, and order
		// In where: handle `or`, `and`, and `like` queries
		stream: function(collectionName, options, stream) {
			spawnConnection(function(connection, cb) {

				// Build find query
				var query = sql.selectQuery(collectionName, options);

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
			}, dbs[collectionName]);
		},

		// Update one or more models in the collection
		update: function(collectionName, options, values, cb) {
			spawnConnection(function(connection, cb) {

				// Escape table name
				var tableName = mysql.escapeId(collectionName);

				// Build query
				var query = 'UPDATE ' + tableName + ' SET ' + sql.criteria(collectionName, values) + ' ';

				query += sql.serializeOptions(collectionName, options);

				// Run query
				connection.query(query, function(err, result) {
					if (err) return cb(err);
					cb(err, result);
				});
			}, dbs[collectionName], cb);
		},

		// Delete one or more models from the collection
		destroy: function(collectionName, options, cb) {
			spawnConnection(function(connection, cb) {

				// Escape table name
				var tableName = mysql.escapeId(collectionName);

				// Build query
				var query = 'DELETE FROM ' + tableName + ' ';

				query += sql.serializeOptions(collectionName, options);

				// Run query
				connection.query(query, function(err, result) {
					cb(err, result);
				});
			}, dbs[collectionName], cb);
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
				return cb(err);
			}

			// console.log("Provisioned new connection.");
			// handleDisconnect(connection, config);

			logic(connection, function(err, result) {
				if (err) {
					console.error("Logic error in mySQL ORM.");
					console.error(err);
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
			// 	return;
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
		return _.extend(config, {
			host: config.host,
			user: config.user,
			password: config.password,
			database: config.database
		});
	}

	return adapter;
})();
