/*---------------------------------------------------------------
	:: waterline-mysql
	-> adapter
---------------------------------------------------------------*/

// Dependencies
var async = require('async');
var _ = require('underscore');
_.str = require('underscore.string');
var mysql = require('mysql');

function MySQLAdapter() {

	// Local cache of schema
	var schema = {};

	var adapter = {

		config: {
			host: 'localhost',
			user: 'waterline',
			password: 'abc123',
			database: 'waterline'
		},

		// Initialize the underlying data model
		initialize: function(cb) {
			var self = this;
			this.pool = mysql.createPool(adapter.config);
			cb();
		},

		teardown: function (cb) {
			var my = this;

			// TODO: Drain pool
			
			cb && cb();
		},


		// Fetch the schema for a collection
		// (contains attributes and autoIncrement value)
		describe: function (collectionName, cb) {
			pool(function __DESCRIBE__ (connection, cb) {
				collectionName = mysql.escapeId(collectionName);
				var query = 'DESCRIBE '+collectionName;
				connection.query(query, function __DESCRIBE__ (err, result) {
					if (err) {
						if (err.code === 'ER_NO_SUCH_TABLE') {
							result = null;
						}
						else return cb(err);
					}
					cb(null, result);
				});
			}, cb);
		},

		// Create a new collection
		define: function(collectionName, attributes, cb) {
			pool(function __DEFINE__ (connection, cb) {

				// Escape table name
				collectionName = mysql.escapeId(collectionName);
				
				// Iterate through each attribute, building a query string
				var $schema = sql.schema(collectionName, attributes);

				// Build query
				var query = 
					'CREATE TABLE ' + collectionName + ' (' +
						$schema + 
					')';

				// Run query
				connection.query(query, function __DEFINE__(err, result) {
					if (err) return cb(err);
					cb(null, result);
				});
			}, cb);
		},

		// Drop an existing collection
		drop: function(collectionName, cb) {
			pool(function __DROP__ (connection, cb) {
				
				// Escape table name
				collectionName = mysql.escapeId(collectionName);
				
				// Build query
				var query = 'DROP TABLE ' + collectionName;

				// Run query
				connection.query(query, function __DROP__(err,result) {
					if (err) {
						if (err.code === 'ER_BAD_TABLE_ERROR') {
							result = null;
						}
						else return cb(err);
					}
					cb(null, result);
				});
			}, cb);
		},

		// No custom alter necessary-- alter can be performed by using the other methods
		// you probably want to use the default in waterline core since this can get complex
		// (that is unless you want some enhanced functionality-- then please be our guest!)


		// Create one or more new models in the collection
		create: function(collectionName, data, cb) {
			pool(function (connection, cb) {
				
				// Escape table name
				var tableName = mysql.escapeId(collectionName);
				
				// Build query
				var query = 
					'INSERT INTO ' + tableName + ' ' +
					'(' +
						sql.attributes(collectionName, data) +
					')' +
					' VALUES (' +
						sql.values(collectionName, data) +
					')';

				// Run query
				connection.query(query, function (err, result) {
					
					// Build model to return
					var model = _.extend({}, data, {

						// TODO: look up the autoIncrement attribute and increment that instead of assuming `id`
						id: result.insertId
					});

					cb(err, model);
				});
			}, cb);
		},

		// Find one or more models from the collection
		// using where, limit, skip, and order
		// In where: handle `or`, `and`, and `like` queries
		find: function(collectionName, options, cb) {
			pool(function (connection, cb) {
				
				// Escape table name
				var tableName = mysql.escapeId(collectionName);
				
				// Build query
				var query = 
					'SELECT * FROM ' + tableName + ' ';

				if (options.where) {
					query += 
						'WHERE ' + 
						sql.criteria(collectionName, options.where) + ' ';
				}

				if (options.sort) {
					query += 'ORDER BY ';

					// Sort through each sort attribute criteria
					_.each(options.sort,function (direction, attrName) {

						query += sql.prepareAttribute(collectionName, null, attrName) + ' ';

						// Basic MongoDB-style numeric sort direction
						if (direction === 1) {
							query += 'ASC ';
						}
						else {
							query += 'DESC ';
						}
					});
				}

				if (options.limit) {
					query += 'LIMIT ' + options.limit + ' ';
				}
				else {
					// Some MySQL hackery here.  For details, see: 
					// http://stackoverflow.com/questions/255517/mysql-offset-infinite-rows
					query += 'LIMIT 18446744073709551610 ';
				}

				if (options.skip) {
					query += 'OFFSET ' + options.skip + ' ';
				}

				// Run query
				connection.query(query, function (err, result) {
					cb(err, result);
				});
			}, cb);
		},

		// Update one or more models in the collection
		update: function(collectionName, options, values, cb) {
			pool(function (connection, cb) {
				
				// Escape table name
				var tableName = mysql.escapeId(collectionName);
				
				// Build query
				var query = 
					'UPDATE ' + tableName + ' SET ' +
					sql.criteria(collectionName, values) + ' ';
				
				if (options.where) {
					query += 
						'WHERE ' + 
						sql.criteria(collectionName, options.where);
				}

				// Run query
				connection.query(query, function (err, result) {
					cb(err, result);
				});
			}, cb);
		},

		// Delete one or more models from the collection
		destroy: function(collectionName, options, cb) {
			pool(function (connection, cb) {
				
				// Escape table name
				var tableName = mysql.escapeId(collectionName);
				
				// Build query
				var query = 
					'DELETE FROM ' + tableName + ' ';

				if (options.where) {
					query += 
						'WHERE ' + 
						sql.criteria(collectionName, options.where);
				}

				// Run query
				connection.query(query, function (err, result) {
					cb(err, result);
				});
			}, cb);
		},


		// Identity is here to facilitate unit testing
		// (this is optional and normally automatically populated based on filename)
		identity: 'waterline-mysql'
	};



	//////////////                 //////////////////////////////////////////
	////////////// Private Methods //////////////////////////////////////////
	//////////////                 //////////////////////////////////////////

	var sql = {
		// Create a schema csv for a DDL query
		schema: function (collectionName, attributes) {
			return sql.build(collectionName, attributes, sql._schema);
		},
		_schema: function (collectionName, attribute, attrName) {
			attrName = mysql.escapeId(attrName);
			var type = sqlTypeCast(attribute.type);
			return attrName + ' ' + 
				type + ' ' + 
				( attribute.autoIncrement ? 'NOT NULL AUTO_INCREMENT, ' +
					'PRIMARY KEY('+ attrName +')' : '');
		},

		// Create an attribute csv for a DQL query
		attributes: function (collectionName, attributes) {
			return sql.build(collectionName, attributes, sql.prepareAttribute);
		},
		
		// Create a value csv for a DQL query
		values: function (collectionName, values) {
			return sql.build(collectionName, values, sql.prepareValue);
		},

		// Create a WHERE criteria snippet for a DQL query
		criteria: function (collectionName, values) {
			return sql.build(collectionName, values, sql.prepareCriterion);
		},

		prepareCriterion: function (collectionName, value, attrName) {
			var attrStr = sql.prepareAttribute(collectionName, value, attrName);
			var valueStr = sql.prepareValue(collectionName, value, attrName);
			return attrStr + "=" + valueStr;
		},

		prepareValue: function (collectionName, value, attrName) {

			// Cast DATE to SQL
			if (adapter.schema[collectionName][attrName].type === 'DATE') {
				value = toSqlDate(value);
			}

			// Escape (also wraps in quotes)
			return mysql.escape(value);
		},

		prepareAttribute: function (collectionName, value, attrName) {
			return mysql.escapeId(attrName);
		},

		

		// Put together the CSV aggregation
		build: function (collectionName, collection, fn) {
			var $sql = '';
			_.each(collection, function (value, key) {
				$sql += fn(collectionName, value, key);

				// (always append comma)
				$sql += ', ';
			});

			// (then remove final one)
			return _.str.rtrim($sql, ', ');
		}
	};

	function wrapInQuotes (val) {
		return '"'+ val + '"';
	}

	function toSqlDate (date) {
		return [
			[
				date.getFullYear(),
				((date.getMonth() < 9 ? '0' : '') + (date.getMonth()+1)),
				((date.getDate() < 10 ? '0' : '') + date.getDate())
			].join("-"),
			date.toLocaleTimeString()
		].join(" ");
	}


	// Wrap a function in the logic necessary to provision a connection from the pool
	function pool (logic, cb) {
		adapter.pool.getConnection(function (err, connection) {
			if (err) return cb(err);
			logic(connection, function (err, result) {
				connection.end();
				cb(err,result);
			});
		});
	}

	// Cast waterline types into SQL data types
	function sqlTypeCast (type) {
		type = type.toLowerCase();

		switch (type) {
			case 'string'	: return 'TEXT';
			
			case 'int':
			case 'integer'	: return 'INT';
			
			case 'date'		: return 'DATE';
		}
	}












	// Run criteria query against data aset
	function applyFilter(data, criteria) {
		if(!data) return data;
		else {
			return _.filter(data, function(model) {
				return matchSet(model, criteria);
			});
		}
	}

	function applySort(data, sort) {
		if (!sort || !data) return data;
		else {
			var sortedData = _.clone(data);

			// Sort through each sort attribute criteria
			_.each(sort,function (direction, attrName) {

				var comparator;

				// Basic MongoDB-style numeric sort direction
				if (direction === 1 || direction === -1) {
					comparator = function (model) {
						return model[attrName];
					};
				}
				else comparator = comparator;

				// Actually sort data
				sortedData = _.sortBy(sortedData,comparator);


				// Reverse it if necessary (if -1 direction specified)
				if (direction === -1) sortedData.reverse();
			});
			return sortedData;
		}
	}
	// Ignore the first *skip* models
	function applySkip(data, skip) {
		if (!skip || !data) return data;
		else {
			return _.rest(data,skip);
		}
	}

	function applyLimit(data, limit) {
		if (!limit || !data) return data;
		else {
			return _.first(data,limit);
		}
	}

	// Find models in data which satisfy the options criteria, 
	// then return their indices in order
	function getMatchIndices(data, options) {
		// Remember original indices
		var origIndexKey = '_waterline_dirty_origindex';
		var matches = _.clone(data);
		_.each(matches,function (model, index) {
			// Determine origIndex key
			// while (model[origIndexKey]) { origIndexKey = '_' + origIndexKey; }
			model[origIndexKey] = index;
		});

		// Query and return result set using criteria
		matches = applyFilter(matches, options.where);
		matches = applySort(matches, options.sort);
		matches = applySkip(matches, options.skip);
		matches = applyLimit(matches, options.limit);
		var matchIndices = _.pluck(matches,origIndexKey);

		// Remove original index key which is keeping track of the index in the unsorted data
		_.each(data, function (datum) {
			delete datum[origIndexKey];
		});
		return matchIndices;
	}


	// Match a model against each criterion in a criteria query

	function matchSet(model, criteria) {

		// Null or {} WHERE query always matches everything
		if(!criteria || _.isEqual(criteria,{})) return true;

		// By default, treat entries as AND
		return _.all(criteria,function(criterion,key) {
			return matchItem(model, key, criterion);
		});
	}


	function matchOr(model, disjuncts) {
		var outcome = false;
		_.each(disjuncts, function(criteria) {
			if(matchSet(model, criteria)) outcome = true;
		});
		return outcome;
	}

	function matchAnd(model, conjuncts) {
		var outcome = true;
		_.each(conjuncts, function(criteria) {
			if(!matchSet(model, criteria)) outcome = false;
		});
		return outcome;
	}

	function matchLike(model, criteria) {
		for(var key in criteria) {

			// Check that criterion attribute and is at least similar to the model's value for that attr
			if(!model[key] || !_.str.include(model[key],criteria[key])) {
				return false;
			}
		}
		return true;
	}

	function matchNot(model, criteria) {
		return !matchSet(model, criteria);
	}

	function matchItem(model, key, criterion) {

		if(key.toLowerCase() === 'or') {
			return matchOr(model, criterion);
		} else if(key.toLowerCase() === 'not') {
			return matchNot(model, criterion);
		} else if(key.toLowerCase() === 'and') {
			return matchAnd(model, criterion);
		} else if(key.toLowerCase() === 'like') {
			return matchLike(model, criterion);
		} 
		// IN query
		else if (_.isArray(criterion)) {
			return _.any(criterion, function (val){
				return model[key] === val;
			});
		}
		// ensure the key attr exists in model
		else if (_.isUndefined(model[key])) {
			return false;
		}

		// ensure the key attr matches model attr in model
		else if((model[key] !== criterion)) {
			return false;
		}
		// Otherwise this is a match
		return true;
	}


	return adapter;
}

// Public API
//	* NOTE: The public API for adapters is a function that can be passed a set of options
//	* It returns the complete adapter, augmented with the options provided
module.exports = function (options) {
	var adapter = new MySQLAdapter();
	adapter.config = _.extend(adapter.config, options || {});
	return adapter;
};