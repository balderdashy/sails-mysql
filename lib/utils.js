/**
 * Utility Functions
 */

// Dependencies
var _ = require('underscore');
url = require('url');
// Module Exports

var utils = module.exports = {

/**
   * Parse URL string from config
   *
   * Parse URL string into connection config parameters
   */
  parseUrl: function (config) {
    if(!_.isString(config.url)) return config;
      
      var obj = url.parse(config.url);

      config.host = obj.hostname || config.host;
      config.port = obj.port || config.port;

      if(_.isString(obj.path)) {
        config.database = obj.path.split("/")[1] || config.database;
      }

      if(_.isString(obj.auth)) {
        config.user = obj.auth.split(":")[0] || config.user;
        config.password = obj.auth.split(":")[1] || config.password;
      }
      return config;
  }

};

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
      if(criteria.sum instanceof Array) {
        criteria.sum.forEach(function(opt){
          query += 'SUM(' + opt + ') AS ' + opt + ', ';
        });

      } else {
        query += 'SUM(' + criteria.sum + ') AS ' + criteria.sum + ', ';
      }
    }

    // Handle AVG (casting to float to fix percision with trailing zeros)
    if (criteria.average) {
      if(criteria.average instanceof Array) {
        criteria.average.forEach(function(opt){
          query += 'AVG(' + opt + ') AS ' + opt + ', ';
        });

      } else {
        query += 'AVG(' + criteria.average + ') AS ' + criteria.average + ', ';
      }
    }

    // Handle MAX
    if (criteria.max) {
      if(criteria.max instanceof Array) {
        criteria.max.forEach(function(opt){
          query += 'MAX(' + opt + ') AS ' + opt + ', ';
        });

      } else {
        query += 'MAX(' + criteria.max + ') AS ' + criteria.max + ', ';
      }
    }

    // Handle MIN
    if (criteria.min) {
      if(criteria.min instanceof Array) {
        criteria.min.forEach(function(opt){
          query += 'MIN(' + opt + ') AS ' + opt + ', ';
        });

      } else {
        query += 'MIN(' + criteria.min + ') AS ' + criteria.min + ', ';
      }
    }

    // trim trailing comma
    query = query.slice(0, -2) + ' ';

    // Add FROM clause
    return query += 'FROM ' + table + ' ';
  }

  // Else select ALL
  return 'SELECT * FROM ' + table + ' ';
};
