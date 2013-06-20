/**
 * Dependencies
 */

var _ = require('underscore');

/**
 * Query Prototype
 *
 * Used for various query manipulations
 */

var Query = function(schema) {
  this._schema = _.clone(schema);
  return this;
};

/**
 * Cast special values to proper types.
 *
 * Ex: Array is stored as "[0,1,2,3]" and should be cast to proper
 * array for return values.
 */

Query.prototype.cast = function(values) {
  var self = this,
      _values = _.clone(values);

  Object.keys(values).forEach(function(key) {

    if(!self._schema[key]) return;

    // Lookup schema type
    var type = self._schema[key].type;
    if(!type) return;

    // Attempt to parse Array
    if(type === 'array') {
      try {
        _values[key] = JSON.parse(values[key]);
      } catch(e) {
        return;
      }
    }

  });

  return _values;
};


module.exports = Query;
