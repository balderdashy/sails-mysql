/**
 * Module Dependencies
 */

var mysql = require('mysql');
var _ = require('lodash');


/**
 * Local utility functions related to building SQL queries.
 * Note that most of this has moved into `waterline-sequel`.
 *
 * @type {Dictionary}
 */
var sql = module.exports = {

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

      if(field.primaryKey) {
        memo[attrName].primaryKey = field.primaryKey;
      }

      if(field.unique) {
        memo[attrName].unique = field.unique;
      }

      if(field.indexed) {
        memo[attrName].indexed = field.indexed;
      }

      return memo;
    }, {});
  },

  // @returns ALTER query for adding a column
  addColumn: function (collectionName, attrName, attrDef) {
    // Escape table name and attribute name
    var tableName = mysql.escapeId(collectionName);

    // sails.log.verbose("ADDING ",attrName, "with",attrDef);

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

  // Create a schema csv for a DDL query
  schema: function(collectionName, attributes) {
    return sql.build(collectionName, attributes, sql._schema);
  },

  _schema: function(collectionName, attribute, attrName) {
    attrName = mysql.escapeId(attrName);
    var type = sqlTypeCast(attribute);

    // Process PK field
    if(attribute.primaryKey) {

      var columnDefinition = attrName + ' ' + type;

      // If type is an integer, set auto increment
      if(type === 'TINYINT' || type === 'SMALLINT' || type === 'INT' || type === 'BIGINT') {
        return columnDefinition + ' UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY';
      }

      // Just set NOT NULL on other types
      return columnDefinition + ' NOT NULL PRIMARY KEY';
    }

    // Process NOT NULL field.
    // if notNull is true, set NOT NULL constraint
    var nullPart = '';
    if (attribute.notNull) {
      nullPart = ' NOT NULL ';
    }

    // Process UNIQUE field
    if(attribute.unique) {
      return attrName + ' ' + type + nullPart + ' UNIQUE KEY';
    }

    // Process INDEX field (NON-UNIQUE KEY)
    if(attribute.index) {
      return attrName + ' ' + type + nullPart + ', INDEX(' + attrName + ')';
    }

    return attrName + ' ' + type + ' ' + nullPart;
  },

  // Put together the CSV aggregation
  // separator => optional, defaults to ', '
  // keyOverride => optional, overrides the keys in the dictionary
  //          (used for generating value lists in IN queries)
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
    return String($sql).replace(new RegExp(separator + '+$'), '');
  }
};

// Cast waterline types into SQL data types
function sqlTypeCast(attr) {
  var type;
  var size;
  var expandedType;

  if(_.isObject(attr) && _.has(attr, 'type')) {
    type = attr.type;
  } else {
    type = attr;
  }

  type = type && type.toLowerCase();

  switch (type) {
    case 'string': {
      size = 255; // By default.

      // If attr.size is positive integer, use it as size of varchar.
      if(!Number.isNaN(attr.size) && (parseInt(attr.size) === parseFloat(attr.size)) && (parseInt(attr.size) > 0)) {
        size = attr.size;
      }

      expandedType = 'VARCHAR(' + size + ')';
      break;
    }

    case 'text':
    case 'array':
    case 'json':
      expandedType = 'LONGTEXT';
      break;

    case 'mediumtext':
      expandedType = 'mediumtext';
      break;

    case 'longtext':
      expandedType = 'longtext';
      break;

    case 'boolean':
      expandedType = 'BOOL';
      break;

    case 'int':
    case 'integer': {
      size = 32; // By default

      if(!Number.isNaN(attr.size) && (parseInt(attr.size) === parseFloat(attr.size)) && (parseInt(size) > 0)) {
        size = parseInt(attr.size);
      }

      // MEDIUMINT gets internally promoted to INT so there is no real benefit
      // using it.

      switch (size) {
        case 8:
          expandedType = 'TINYINT';
          break;
        case 16:
          expandedType = 'SMALLINT';
          break;
        case 32:
          expandedType = 'INT';
          break;
        case 64:
          expandedType = 'BIGINT';
          break;
        default:
          expandedType = 'INT';
          break;
      }

      break;
    }

    case 'float':
    case 'double':
      expandedType = 'FLOAT';
      break;

    case 'decimal':
      expandedType = 'DECIMAL';
      break;

    case 'date':
      expandedType = 'DATE';
      break;

    case 'datetime':
      expandedType = 'DATETIME';
      break;

    case 'time':
      expandedType = 'TIME';
      break;

    case 'binary':
      expandedType = 'BLOB';
      break;

    default:
      console.error('Unregistered type given: ' + type);
      expandedType = 'LONGTEXT';
      break;
  }

  return expandedType;
}

// function toSqlDate(date) {

//   date = date.getFullYear() + '-' +
//     ('00' + (date.getMonth()+1)).slice(-2) + '-' +
//     ('00' + date.getDate()).slice(-2) + ' ' +
//     ('00' + date.getHours()).slice(-2) + ':' +
//     ('00' + date.getMinutes()).slice(-2) + ':' +
//     ('00' + date.getSeconds()).slice(-2);

//   return date;
// }

// // Return whether this criteria is valid as an object inside of an attribute
// function validSubAttrCriteria(c) {
//   return _.isObject(c) && (
//   !_.isUndefined(c.not) || !_.isUndefined(c.greaterThan) || !_.isUndefined(c.lessThan) ||
//   !_.isUndefined(c.greaterThanOrEqual) || !_.isUndefined(c.lessThanOrEqual) || !_.isUndefined(c['<']) ||
//   !_.isUndefined(c['<=']) || !_.isUndefined(c['!']) || !_.isUndefined(c['>']) || !_.isUndefined(c['>=']) ||
//   !_.isUndefined(c.startsWith) || !_.isUndefined(c.endsWith) || !_.isUndefined(c.contains) || !_.isUndefined(c.like));
// }
