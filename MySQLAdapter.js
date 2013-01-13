/*---------------------------------------------------------------
	:: DirtyAdapter
	-> adapter

	This disk+memory adapter is for development only!
	Learn more: https://github.com/felixge/node-dirty
---------------------------------------------------------------*/

// Poor man's auto-increment
//	* NOTE: In production databases, auto-increment capabilities 
//	* are built-in.  However, that is not the case with dirty.
//	* This in-memory auto-increment will not scale to a multi-instance / cluster setup.
var statusDb = {};


// Dependencies
var async = require('async');
var _ = require('underscore');
_.str = require('underscore.string');
var dirty = require('dirty');
var parley = require('parley');
var uuid = require('node-uuid');


var adapter = {

	config: {

		// If inMemory is true, all data will be destroyed when the server stops
		// otherwise, it will be written to disk
		inMemory: true,

		// File path for disk file output (when NOT in inMemory mode)
		dbFilePath: './.waterline/dirty.db',

		// String to precede key name for schema defininitions
		schemaPrefix: 'waterline_schema_',

		// String to precede key name for actual data in collection
		dataPrefix: 'waterline_data_'
	},

	// Initialize the underlying data model
	initialize: function(cb) {
		var my = this;

		if(! this.config.inMemory) {

			// Check that dbFilePath file exists and build tree as necessary
			require('fs-extra').touch(this.config.dbFilePath, function(err) {
				if(err) return cb(err);
				my.db = new(dirty.Dirty)(my.config.dbFilePath);

				afterwards();
			});
		} else {
			this.db = new(dirty.Dirty)();
			afterwards();
		}

		function afterwards() {

			// Make logger easily accessible
			my.log = my.config.log;

			// Trigger callback with no error
			my.db.on('load', function() {
				cb();
			});
		}
	},

	// Logic to handle the (re)instantiation of collections
	initializeCollection: function(collectionName, cb) {
		var self = this;

		// Grab current auto-increment value from database and populate it in-memory
		var schema = this.db.get(this.config.schemaPrefix + collectionName);
		statusDb[collectionName] = (schema && schema.autoIncrement) ? schema : {autoIncrement: 1};

		self.getAutoIncrementAttribute(collectionName, function (err,aiAttr) {
			// Check that the resurrected auto-increment value is valid
			self.find(collectionName, {
				where: {
					id: statusDb[collectionName].autoIncrement
				}
			}, function (err, models) {
				if (err) return cb(err);

				// If that model already exists, warn the user and generate the next-best possible auto-increment key
				if (models && models.length) {

					// Find max
					self.find(collectionName, {}, function (err,models) {
						var autoIncrement = _.max(models,function (model){
							return model[aiAttr];
						});
						autoIncrement = autoIncrement && autoIncrement[aiAttr] || 0;
						autoIncrement++;

						self.log.warn("On-disk auto-increment ID corrupt, using: "+autoIncrement+" on attribute:",aiAttr);
						statusDb[collectionName].autoIncrement = autoIncrement;
						cb();
					});
				}
				else cb();
			});
		});
	},

	// find this collection's auto-increment field and return its name
	getAutoIncrementAttribute: function (collectionName, cb) {
		this.describe(collectionName, function (err,attributes) {
			var attrName, done=false;
			_.each(attributes, function(attribute, aname) {
				if(!done && _.isObject(attribute) && attribute.autoIncrement) {
					attrName = aname;
					done = true;
				}
			});

			cb(null, attrName);
		});
	},

	// Logic to handle flushing collection data to disk before the adapter shuts down
	teardownCollection: function(collectionName, cb) {
		var my = this;
		
		// Always go ahead and write the new auto-increment to disc, even though it will be wrong sometimes
		// (this is done so that the auto-increment counter can be "resurrected" when the adapter is restarted from disk)
		var schema = _.extend(this.db.get(this.config.schemaPrefix + collectionName),{
			autoIncrement: statusDb[collectionName].autoIncrement
		});
		this.log.info("Waterline saving "+collectionName+" collection...");
		this.db.set(this.config.schemaPrefix + collectionName, schema, function (err) {
			my.db = null;
			cb && cb(err);
		});
	},


	// Fetch the schema for a collection
	// (contains attributes and autoIncrement value)
	describe: function(collectionName, cb) {	
		this.log.verbose(" DESCRIBING :: " + collectionName);
		var schema = this.db.get(this.config.schemaPrefix + collectionName);
		var attributes = schema && schema.attributes;
		return cb(null, attributes);
	},

	// Create a new collection
	define: function(collectionName, attributes, cb) {
		this.log.verbose(" DEFINING " + collectionName, {
			as: schema
		});
		var self = this;

		var schema = {
			attributes: _.clone(attributes),
			autoIncrement: 1
		};

		// Write schema and status objects
		return self.db.set(this.config.schemaPrefix + collectionName, schema, function(err) {
			if(err) return cb(err);

			// Set in-memory schema for this collection
			statusDb[collectionName] = schema;
			cb();
		});
	},

	// Drop an existing collection
	drop: function(collectionName, cb) {
		var self = this;
		self.log.verbose(" DROPPING " + collectionName);
		return self.db.rm(self.config.dataPrefix + collectionName, function(err) {
			if(err) return cb("Could not drop collection!");
			return self.db.rm(self.config.schemaPrefix + collectionName, cb);
		});
	},

	// No alter necessary-- use the default in waterline core



	// Create one or more new models in the collection
	create: function(collectionName, values, cb) {
		this.log.verbose(" CREATING :: " + collectionName, values);
		values = _.clone(values) || {};
		var dataKey = this.config.dataPrefix + collectionName;
		var data = this.db.get(dataKey);
		var self = this;


		// Lookup schema & status so we know all of the attribute names and the current auto-increment value
		var schema = this.db.get(this.config.schemaPrefix + collectionName);

		// Auto increment fields that need it
		doAutoIncrement(collectionName, schema.attributes, values, this, function (err, values) {
			if (err) return cb(err);

			self.describe(collectionName, function(err, attributes) {
				if(err) return cb(err);

				// Create new model
				// (if data collection doesn't exist yet, create it)
				data = data || [];
				data.push(values);

				// Replace data collection and go back
				self.db.set(dataKey, data, function(err) {
					return cb(err, values);
				});
			});
		});
	},

	// Find one or more models from the collection
	// using where, limit, skip, and order
	// In where: handle `or`, `and`, and `like` queries
	find: function(collectionName, options, cb) {
		var dataKey = this.config.dataPrefix + collectionName;
		var data = this.db.get(dataKey) || [];

		// Get indices from original data which match, in order
		var matchIndices = getMatchIndices(data,options);

		var resultSet = [];
		_.each(matchIndices,function (matchIndex) {
			resultSet.push(data[matchIndex]);
		});
		
		cb(null, resultSet);
	},

	// Update one or more models in the collection
	update: function(collectionName, options, values, cb) {
		this.log.verbose(" UPDATING :: " + collectionName, {
			options: options,
			values: values
		});
		var my = this;

		
		var dataKey = this.config.dataPrefix + collectionName;
		var data = this.db.get(dataKey);

		// Query result set using options
		var matchIndices = getMatchIndices(data,options);

		// Update model(s)
		_.each(matchIndices, function(index) {
			data[index] = _.extend(data[index], values);
		});

		// Get result set for response
		var resultSet = [];
		_.each(matchIndices,function (matchIndex) {
			resultSet.push(data[matchIndex]);
		});

		// Replace data collection and go back
		this.db.set(dataKey, data, function(err) {
			cb(err, resultSet);
		});
	},

	// Delete one or more models from the collection
	destroy: function(collectionName, options, cb) {
		this.log.verbose(" DESTROYING :: " + collectionName, options);

		var dataKey = this.config.dataPrefix + collectionName;
		var data = this.db.get(dataKey);

		// Query result set using options
		var matchIndices = getMatchIndices(data,options);

		
		// Remove model(s)
		// Get result set of only the models that remain
		data = _.reject(data, function(model,index) {
			return _.contains(matchIndices, index);
		});

		// Replace data collection and respond with what's left
		this.db.set(dataKey, data, function(err) {
			cb(err);
		});
	},



	// Identity is here to facilitate unit testing
	// (this is optional and normally automatically populated based on filename)
	identity: 'dirty'
};



//////////////                 //////////////////////////////////////////
////////////// Private Methods //////////////////////////////////////////
//////////////                 //////////////////////////////////////////

// Look for auto-increment field, increment counter accordingly, and return refined value set
function doAutoIncrement (collectionName, attributes, values, ctx, cb) {

	// Determine the attribute names which will be included in the created object
	var attrNames = _.keys(_.extend({}, attributes, values));

	// increment AI fields in values set
	_.each(attrNames, function(attrName) {
		if(_.isObject(attributes[attrName]) && attributes[attrName].autoIncrement) {
			values[attrName] = statusDb[collectionName].autoIncrement;

			// Then, increment the auto-increment counter for this collection
			statusDb[collectionName].autoIncrement++;
		}
	});

	// Return complete values set w/ auto-incremented data
	return cb(null,values);
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
			if (direction === 1 || direction === -1) comparator = function (model) {return model[attrName];};
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

// Public API
//	* NOTE: The public API for adapters is a function that can be passed a set of options
//	* It returns the complete adapter, augmented with the options provided
module.exports = function (options) {
	adapter.config = _.extend(adapter.config, options || {});
	return adapter;
};