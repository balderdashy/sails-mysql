var mysql = require('mysql');
var _ = require('underscore');
_.str = require('underscore.string');

var sql = {

	// Convert mysql format to standard javascript object
	normalizeSchema: function (schema) {
		return _.reduce(schema, function(memo, field) {

			// Marshal mysql DESCRIBE to waterline collection semantics
			var attrName = field.Field;
			var type = field.Type;

			// Remove (n) column-size indicators
			type = type.replace(/\([0-9]+\)$/,'');

			memo[attrName] = {
				type: type,
				defaultsTo: field.Default,
				autoIncrement: field.Extra === 'auto_increment'
			};
			return memo;
		}, {});
	},

	// @returns ALTER query for adding a column
	addColumn: function (collectionName, attrName, attrDef) {
		// Escape table name and attribute name
		var tableName = mysql.escapeId(collectionName);

		sails.log.verbose("ADDING ",attrName, "with",attrDef);

		// Build column definition
		var columnDefinition = sql._schema(collectionName, attrDef, attrName);

		return 'ALTER TABLE ' + tableName + ' ADD ' + columnDefinition;
	},

	// @returns ALTER query for dropping a column
	removeColumn: function (collectionName, attrName) {
		// Escape table name and attribute name
		var tableName = mysql.escapeId(collectionName);
		attrName = mysql.escapeId(attrName);

		return 'ALTER TABLE ' + tableName + ' DROP COLUMN ' + attrName;
	},

	selectQuery: function (collectionName, options) {
		// Escape table name
		var tableName = mysql.escapeId(collectionName);

		// Build query
		return 'SELECT * FROM ' + tableName + ' ' + sql.serializeOptions(collectionName, options);
	},

	insertQuery: function (collectionName, data) {
		// Escape table name
		var tableName = mysql.escapeId(collectionName);

		// Build query
		return 'INSERT INTO ' + tableName + ' ' + '(' + sql.attributes(collectionName, data) + ')' + ' VALUES (' + sql.values(collectionName, data) + ')';
	},

	// Create a schema csv for a DDL query
	schema: function(collectionName, attributes) {
		return sql.build(collectionName, attributes, sql._schema);
	},
	_schema: function(collectionName, attribute, attrName) {
		attrName = mysql.escapeId(attrName);
		var type = sqlTypeCast(attribute.type);
		// return attrName + ' ' + type + ' ' + (attribute.autoIncrement ? 'NOT NULL AUTO_INCREMENT, ' + 'PRIMARY KEY(' + attrName + ')' : '');
		return attrName + ' ' + type + ' ' + (attribute.autoIncrement ? 'NOT NULL AUTO_INCREMENT PRIMARY KEY' : '');
	},

	// Create an attribute csv for a DQL query
	attributes: function(collectionName, attributes) {
		return sql.build(collectionName, attributes, sql.prepareAttribute);
	},

	// Create a value csv for a DQL query
	// key => optional, overrides the keys in the dictionary
	values: function(collectionName, values, key) {
		return sql.build(collectionName, values, sql.prepareValue, ', ', key);
	},

	// Create a WHERE criteria snippet for a DQL query
	criteria: function(collectionName, values) {
		return sql.build(collectionName, values, sql.prepareCriterion);
	},

	prepareCriterion: function(collectionName, value, key, parentKey) {
		// Special sub-attr case
		if (validSubAttrCriteria(value)) {
			return sql.where(collectionName, value, null, key);

		}

		// Build escaped attr and value strings using either the key,
		// or if one exists, the parent key
		var attrStr, valueStr;


		// Special comparator case
		if (parentKey) {

			attrStr = sql.prepareAttribute(collectionName, value, parentKey);
			valueStr = sql.prepareValue(collectionName, value, parentKey);

			// Why don't we strip you out of those bothersome apostrophes?
			var nakedButClean = _.str.trim(valueStr,'\'');

			if (key === '<' || key === 'lessThan') return attrStr + '<' + valueStr;
			else if (key === '<=' || key === 'lessThanOrEqual') return attrStr + '<=' + valueStr;
			else if (key === '>' || key === 'greaterThan') return attrStr + '>' + valueStr;
			else if (key === '>=' || key === 'greaterThanOrEqual') return attrStr + '>=' + valueStr;
			else if (key === '!' || key === 'not') {
				if (value === null) return attrStr + 'IS NOT NULL';
				else return attrStr + '<>' + valueStr;
			} 
			else if (key === 'like') return attrStr + ' LIKE \'' + nakedButClean + '\'';
			else if (key === 'contains') return attrStr + ' LIKE \'%' + nakedButClean + '%\'';
			else if (key === 'startsWith') return attrStr + ' LIKE \'' + nakedButClean + '%\'';
			else if (key === 'endsWith') return attrStr + ' LIKE \'%' + nakedButClean + '\'';
			else throw new Error('Unknown comparator: ' + key);
		} else {
			attrStr = sql.prepareAttribute(collectionName, value, key);
			valueStr = sql.prepareValue(collectionName, value, key);

			// Special IS NULL case
			if (_.isNull(value)) {
				return attrStr + " IS NULL";
			} else return attrStr + "=" + valueStr;
		}
	},

	prepareValue: function(collectionName, value, attrName) {

		// Cast dates to SQL
		if (_.isDate(value)) {
			value = toSqlDate(value);
		}

		// Cast functions to strings
		if (_.isFunction(value)) {
			value = value.toString();
		}

		// Escape (also wraps in quotes)
		return mysql.escape(value);
	},

	prepareAttribute: function(collectionName, value, attrName) {
		return mysql.escapeId(attrName);
	},

	// Starting point for predicate evaluation
	// parentKey => if set, look for comparators and apply them to the parent key
	where: function(collectionName, where, key, parentKey) {
		return sql.build(collectionName, where, sql.predicate, ' AND ', undefined, parentKey);
	},

	// Recursively parse a predicate calculus and build a SQL query
	predicate: function(collectionName, criterion, key, parentKey) {
		var queryPart = '';


		if (parentKey) {
			return sql.prepareCriterion(collectionName, criterion, key, parentKey);
		}

		// OR
		if (key.toLowerCase() === 'or') {
			queryPart = sql.build(collectionName, criterion, sql.where, ' OR ');
			return ' ( ' + queryPart + ' ) ';
		}

		// AND
		else if (key.toLowerCase() === 'and') {
			queryPart = sql.build(collectionName, criterion, sql.where, ' AND ');
			return ' ( ' + queryPart + ' ) ';
		}

		// IN
		else if (_.isArray(criterion)) {
			queryPart = sql.prepareAttribute(collectionName, null, key) + " IN (" + sql.values(collectionName, criterion, key) + ")";
			return queryPart;
		}

		// LIKE
		else if (key.toLowerCase() === 'like') {
			return sql.build(collectionName, criterion, function(collectionName, value, attrName) {
				var attrStr = sql.prepareAttribute(collectionName, value, attrName);


				// TODO: Handle regexp criterias
				if (_.isRegExp(value)) {
					throw new Error('RegExp LIKE criterias not supported by the MySQLAdapter yet.  Please contribute @ http://github.com/balderdashy/sails-mysql');
				}

				var valueStr = sql.prepareValue(collectionName, value, attrName);

				// Handle escaped percent (%) signs [encoded as %%%]
				valueStr = valueStr.replace(/%%%/g, '\\%');

				return attrStr + " LIKE " + valueStr;
			}, ' AND ');
		}

		// NOT
		else if (key.toLowerCase() === 'not') {
			throw new Error('NOT not supported yet!');
		}

		// Basic criteria item
		else {
			return sql.prepareCriterion(collectionName, criterion, key);
		}

	},

	serializeOptions: function(collectionName, options) {
		var queryPart = '';

		if (options.where) {
			queryPart += 'WHERE ' + sql.where(collectionName, options.where) + ' ';
		}

		if (options.sort) {
			queryPart += 'ORDER BY ';

			// Sort through each sort attribute criteria
			_.each(options.sort, function(direction, attrName) {

				queryPart += sql.prepareAttribute(collectionName, null, attrName) + ' ';

				// Basic MongoDB-style numeric sort direction
				if (direction === 1) {
					queryPart += 'ASC ';
				} else {
					queryPart += 'DESC ';
				}
			});
		}

		if (options.limit) {
			queryPart += 'LIMIT ' + options.limit + ' ';
		} else {
			// Some MySQL hackery here.  For details, see: 
			// http://stackoverflow.com/questions/255517/mysql-offset-infinite-rows
			queryPart += 'LIMIT 18446744073709551610 ';
		}

		if (options.skip) {
			queryPart += 'OFFSET ' + options.skip + ' ';
		}

		return queryPart;
	},

	// Put together the CSV aggregation
	// separator => optional, defaults to ', '
	// keyOverride => optional, overrides the keys in the dictionary 
	//					(used for generating value lists in IN queries)
	// parentKey => key of the parent to this object
	build: function(collectionName, collection, fn, separator, keyOverride, parentKey) {
		separator = separator || ', ';
		var $sql = '';
		_.each(collection, function(value, key) {
			$sql += fn(collectionName, value, keyOverride || key, parentKey);

			// (always append separator)
			$sql += separator;
		});

		// (then remove final one)
		return _.str.rtrim($sql, separator);
	}
};

// Cast waterline types into SQL data types
function sqlTypeCast(type) {
	type = type && type.toLowerCase();

	switch (type) {
		case 'string':
		case 'text':
			return 'TEXT';

		case 'boolean':
		case 'int':
		case 'integer':
			return 'INT';

		case 'float':
		case 'double':
			return 'FLOAT';

		case 'date':
			return 'DATE';
	
		case 'datetime':
			return 'DATETIME';

		default:
			console.error("Unregistered type given: " + type);
			return "TEXT";
	}
}

function wrapInQuotes(val) {
	return '"' + val + '"';
}

function toSqlDate(date) {
	return [[date.getFullYear(), ((date.getMonth() < 9 ? '0' : '') + (date.getMonth() + 1)), ((date.getDate() < 10 ? '0' : '') + date.getDate())].join("-"), date.toLocaleTimeString()].join(" ");
}

// Return whether this criteria is valid as an object inside of an attribute
function validSubAttrCriteria(c) {
	return _.isObject(c) && (
	c.not || c.greaterThan || c.lessThan || c.greaterThanOrEqual || c.lessThanOrEqual || c['<'] || c['<='] || c['!'] || c['>'] || c['>='] || c.startsWith || c.endsWith || c.contains || c.like);
}

module.exports = sql;