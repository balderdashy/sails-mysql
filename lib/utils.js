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

  // Cast functions to strings
  if (_.isFunction(value)) {
    value = value.toString();
  }

  // Store Arrays and Objects as strings
  if ( _.isObject(value) ) {
    try {
      value = JSON.stringify(value);
    } catch (e) {
      // just keep the value and let the db handle an error
      value = value;
    }
  }

  return value;
};
