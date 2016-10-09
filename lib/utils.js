/**
 * Module dependencies
 */

var url = require('url');
var _ = require('lodash');
var mysql = require('mysql');



// Module Exports

var utils = module.exports = {

  /**
   * Parse the connection URL string lodged within the provided
   * datastore config.
   *
   * @param  {Dictionary} config [description]
   * @return {[type]}        [description]
   */
  parseUrl: function (config) {
    if(!_.isString(config.url)) {
      return config;
    }

    var obj = url.parse(config.url);

    config.host = obj.hostname || config.host;
    config.port = obj.port || config.port;

    if(_.isString(obj.pathname)) {
      config.database = obj.pathname.split('/')[1] || config.database;
    }

    if(_.isString(obj.auth)) {
      config.user = obj.auth.split(':')[0] || config.user;
      config.password = obj.auth.split(':')[1] || config.password;
    }
    return config;
  },


  /**
   * Prepare the provided value to be stored in a MySQL database.
   *
   * @param  {[type]} value [description]
   * @return {[type]}       [description]
   */
  prepareValue: function(value) {

    if(_.isUndefined(value) || value === null) {
      return value;
    }

    // Cast functions to strings
    if (_.isFunction(value)) {
      value = value.toString();
    }

    // Store Arrays and Objects as strings
    if (_.isArray(value) || value.constructor && value.constructor.name === 'Object') {
      try {
        value = JSON.stringify(value);
      } catch (e) {
        // just keep the value and let the db handle an error
        value = value;
      }
    }

    // Cast dates to SQL
    if (_.isDate(value)) {
      value = utils.toSqlDate(value);
    }

    return mysql.escape(value);
  },


  /**
   * [toSqlDate description]
   * @param  {[type]} date [description]
   * @return {[type]}      [description]
   */
  toSqlDate: function toSqlDate(date) {

    date = date.getFullYear() + '-' +
      ('00' + (date.getMonth()+1)).slice(-2) + '-' +
      ('00' + date.getDate()).slice(-2) + ' ' +
      ('00' + date.getHours()).slice(-2) + ':' +
      ('00' + date.getMinutes()).slice(-2) + ':' +
      ('00' + date.getSeconds()).slice(-2);

    return date;
  }

};
