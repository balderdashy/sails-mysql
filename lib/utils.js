/**
 * Utility Functions
 */

// Dependencies
var mysql = require('mysql'),
    _ = require('lodash');

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

utils.buildSelectStatement = function(criteria, table, schemaDefs) {

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
    return query += 'FROM `' + table + '` ';
  }

  /**
   * If no aggregate options lets just build a normal query
   */


  // Add all keys to the select statement for this table
  query += 'SELECT ';

  var selectKeys = [],
      joinSelectKeys = [];

  if ( !schemaDefs[table] ) throw new Error('Schema definition missing for table: `'+table+'`');

  _( schemaDefs[table] ).forEach(function(schemaDef, key) {
    selectKeys.push({ table: table, key: key });
  });

  // Check for joins
  if(criteria.joins || criteria.join) {

    var joins = criteria.joins || criteria.join;

    joins.forEach(function(join) {
      if(!join.select) return;

      Object.keys(schemaDefs[join.child.toLowerCase()]).forEach(function(key) {
        var _join = _.cloneDeep(join);
        _join.key = key;
        joinSelectKeys.push(_join);
      });

      // Remove the foreign key for this join from the selectKeys array
      selectKeys = selectKeys.filter(function(select) {
        var keep = true;
        if(select.key === join.parentKey && join.removeParentKey) keep = false;
        return keep;
      });
    });
  }

  // Add all the columns to be selected that are not joins
  selectKeys.forEach(function(select) {
    query += '`' + select.table + '`.`' + select.key + '`, ';
  });

  // Add all the columns from the joined tables
  joinSelectKeys.forEach(function(select) {

    // Create an alias by prepending the child table with the alias of the join
    var alias = select.alias.toLowerCase() + '_' + select.child.toLowerCase();

    // If this is a belongs_to relationship, keep the foreign key name from the AS part
    // of the query. This will result in a selected column like: "user"."id" AS "user_id__id"
    if(select.model) {
      return query += mysql.escapeId(alias) + '.' + mysql.escapeId(select.key) + ' AS ' +
                      mysql.escapeId(select.parentKey + '__' + select.key) + ', ';
    }

    // If a junctionTable is used, the child value should be used in the AS part of the
    // select query.
    if(select.junctionTable) {
      return query += mysql.escapeId(alias) + '.' + mysql.escapeId(select.key) + ' AS ' +
                      mysql.escapeId(select.alias + '__' + select.key) + ', ';
    }

    // Else if a hasMany attribute is being selected, use the alias plus the child
    return query += mysql.escapeId(alias) + '.' + mysql.escapeId(select.key) + ' AS ' +
                    mysql.escapeId(select.alias + '__' + select.key) + ', ';
  });

  // Remove the last comma
  query = query.slice(0, -2) + ' FROM `' + table + '` ';

  return query;
};


/**
 * Group Results into an Array
 *
 * Groups values returned from an association query into a single result.
 * For each collection association the object returned should have an array under
 * the user defined key with the joined results.
 *
 * @param {Array} results returned from a query
 * @return {Object} a single values object
 */

utils.group = function(values) {

  var self = this,
      joinKeys = [],
      _value;

  if(!Array.isArray(values)) return values;

  // Grab all the keys needed to be grouped
  var associationKeys = [];

  values.forEach(function(value) {
    Object.keys(value).forEach(function(key) {
      key = key.split('__');
      if(key.length === 2) associationKeys.push(key[0].toLowerCase());
    });
  });

  associationKeys = _.unique(associationKeys);

  // Store the values to be grouped by id
  var groupings = {};

  values.forEach(function(value) {

    // add to groupings
    if(!groupings[value.id]) groupings[value.id] = {};

    associationKeys.forEach(function(key) {
      if(!Array.isArray(groupings[value.id][key])) groupings[value.id][key] = [];
      var props = {};

      Object.keys(value).forEach(function(prop) {
        var attr = prop.split('__');
        if(attr.length === 2 && attr[0] === key) {
          props[attr[1]] = value[prop];
          delete value[prop];
        }
      });

      // Don't add empty records that come from a left join
      var empty = true;

      Object.keys(props).forEach(function(prop) {
        if(props[prop] !== null) empty = false;
      });

      if(!empty) groupings[value.id][key].push(props);
    });
  });

  var _values = [];

  values.forEach(function(value) {
    var unique = true;

    _values.forEach(function(_value) {
      if(_value.id === value.id) unique = false;
    });

    if(!unique) return;

    Object.keys(groupings[value.id]).forEach(function(key) {
      value[key] = _.uniq(groupings[value.id][key], 'id');
    });

    _values.push(value);
  });

  return _values;
};
