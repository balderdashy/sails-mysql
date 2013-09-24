/**
 * Utility Functions
 */

// Dependencies
var _ = require('underscore');

// Module Exports

var utils = module.exports = {};

/**
 * Prepare values
 *
 * Transform a JS date to SQL date and functions
 * to strings.
 */

utils.prepareValue = function(value) {

  if(!value) return value;

  // Cast functions to strings
  if (_.isFunction(value)) {
    value = value.toString();
  }

  // Store Arrays and Objects as strings
  if (Array.isArray(value) || value.constructor && value.constructor.name === 'Object') {
    try {
      value = JSON.stringify(value);
    } catch (e) {
      // just keep the value and let the db handle an error
      value = value;
    }
  }

  return value;
};

/**
 * Builds a Select statement determining if Aggeregate options are needed.
 */

function convertCriteria(criteria) {
  if(typeof(criteria) == 'string') {
    return {
      field : criteria,
      alias : criteria
    }
  } else {
    if(criteria.field && criteria.alias) {
      return criteria;
    } else {
      throw 'invalid criteria ' + criteria;
    }
  }
}

function convertAggregation(aggregation, criteria) {
    if(criteria instanceof Array) {
		var query = '';
        criteria.forEach(function(opt) {
            opt = convertCriteria(opt);
            query += aggregation + '(' + opt.field + ') AS ' + opt.alias + ', ';
        });
		return query;
    } else {
        criteria = convertCriteria(criteria);
        return aggregation + '(' + criteria.field + ') AS ' + criteria.alias + ', ';
    }
}

utils.buildSelectStatement = function(criteria, table) {

  var query = '';

  if(criteria.groupBy || criteria.sum || criteria.average || criteria.min || criteria.max) {
    query = 'SELECT ';

    // Append groupBy columns to select statement
    if(criteria.groupBy) {
      if(criteria.groupBy instanceof Array) {
        criteria.groupBy.forEach(function(opt){
          query += opt + ', ';
        });

      } else {
        query += criteria.groupBy + ', ';
      }
    }

    // Handle SUM
    if (criteria.sum) {
      query += convertAggregation('SUM', criteria.sum);
    }

    // Handle AVG (casting to float to fix percision with trailing zeros)
    if (criteria.average) {
      query += convertAggregation('AVG', criteria.average);
    }

    // Handle MAX
    if (criteria.max) {
      query += convertAggregation('MAX', criteria.max);
    }

    // Handle MIN
    if (criteria.min) {
      query += convertAggregation('MIN', criteria.min);
    }

    // trim trailing comma
    query = query.slice(0, -2) + ' ';

    // Add FROM clause
    return query += 'FROM ' + table + ' ';
  }

  // Else select ALL
  return 'SELECT * FROM ' + table + ' ';
};
