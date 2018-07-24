//  ██████╗ ██╗   ██╗███╗   ██╗     ██████╗ ██╗   ██╗███████╗██████╗ ██╗   ██╗
//  ██╔══██╗██║   ██║████╗  ██║    ██╔═══██╗██║   ██║██╔════╝██╔══██╗╚██╗ ██╔╝
//  ██████╔╝██║   ██║██╔██╗ ██║    ██║   ██║██║   ██║█████╗  ██████╔╝ ╚████╔╝
//  ██╔══██╗██║   ██║██║╚██╗██║    ██║▄▄ ██║██║   ██║██╔══╝  ██╔══██╗  ╚██╔╝
//  ██║  ██║╚██████╔╝██║ ╚████║    ╚██████╔╝╚██████╔╝███████╗██║  ██║   ██║
//  ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝     ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝
//
// Send a Native Query to the datastore and gracefully handle errors.

var _ = require('@sailshq/lodash');
var MySQL = require('machinepack-mysql');
var releaseConnection = require('../connection/release-connection');

module.exports = function runQuery(options, cb) {
  //  ╦  ╦╔═╗╦  ╦╔╦╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┌┬┐┬┌─┐┌┐┌┌─┐
  //  ╚╗╔╝╠═╣║  ║ ║║╠═╣ ║ ║╣   │ │├─┘ │ ││ ││││└─┐
  //   ╚╝ ╩ ╩╩═╝╩═╩╝╩ ╩ ╩ ╚═╝  └─┘┴   ┴ ┴└─┘┘└┘└─┘
  if (_.isUndefined(options) || !_.isPlainObject(options)) {
    throw new Error('Invalid options argument. Options must contain: connection, nativeQuery, and leased.');
  }

  if (!_.has(options, 'connection') || !_.isObject(options.connection)) {
    throw new Error('Invalid option used in options argument. Missing or invalid connection.');
  }

  if (!_.has(options, 'nativeQuery')) {
    throw new Error('Invalid option used in options argument. Missing or invalid nativeQuery.');
  }


  //  ╦═╗╦ ╦╔╗╔  ┌┐┌┌─┐┌┬┐┬┬  ┬┌─┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
  //  ╠╦╝║ ║║║║  │││├─┤ │ │└┐┌┘├┤   │─┼┐│ │├┤ ├┬┘└┬┘
  //  ╩╚═╚═╝╝╚╝  ┘└┘┴ ┴ ┴ ┴ └┘ └─┘  └─┘└└─┘└─┘┴└─ ┴
  MySQL.sendNativeQuery({
    connection: options.connection,
    nativeQuery: options.nativeQuery,
    valuesToEscape: options.valuesToEscape,
    meta: options.meta
  })
  .switch({
    // If there was an error, check if the connection should be
    // released back into the pool automatically.
    error: function error(err) {
      if (!options.disconnectOnError) {
        return cb(err);
      }

      releaseConnection(options.connection, options.leased, function releaseConnectionCb(err) {
        return cb(err);
      });
    },
    // If the query failed, try and parse it into a normalized format and
    // release the connection if needed.
    queryFailed: function queryFailed(report) {
      // Parse the native query error into a normalized format
      var parsedError;
      try {
        parsedError = MySQL.parseNativeQueryError({
          nativeQueryError: report.error
        }).execSync();
      } catch (e) {
        if (!options.disconnectOnError) {
          return cb(e);
        }

        releaseConnection(options.connection, function releaseConnectionCb() {
          return cb(e);
        });
        return;
      }

      // If the catch all error was used, return an error instance instead of
      // the footprint.
      var catchAllError = false;

      if (parsedError.footprint.identity === 'catchall') {
        catchAllError = true;
      }

      // If this shouldn't disconnect the connection, just return the normalized
      // error with the footprint.
      if (!options.disconnectOnError) {
        if (catchAllError) {
          return cb(report.error);
        }

        return cb(parsedError);
      }

      releaseConnection(options.connection, false, function releaseConnectionCb() {
        if (catchAllError) {
          return cb(report.error);
        }

        return cb(parsedError);
      });
    },
    success: function success(report) {
      // If a custom primary key was used and the record has an `insert` query
      // type, build a manual insert report because we don't have the actual
      // value that was used.
      if (options.customPrimaryKey) {
        return cb(null, {
          result: {
            inserted: options.customPrimaryKey
          }
        });
      }


      //  ╔═╗╔═╗╦═╗╔═╗╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬  ┬─┐┌─┐┌─┐┬ ┬┬ ┌┬┐┌─┐
      //  ╠═╝╠═╣╠╦╝╚═╗║╣   │─┼┐│ │├┤ ├┬┘└┬┘  ├┬┘├┤ └─┐│ ││  │ └─┐
      //  ╩  ╩ ╩╩╚═╚═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴   ┴└─└─┘└─┘└─┘┴─┘┴ └─┘
      // If there was a query type given, parse the results.
      var queryResults = report.result;
      if (options.queryType) {
        try {
          queryResults = MySQL.parseNativeQueryResult({
            queryType: options.queryType,
            nativeQueryResult: report.result
          }).execSync();
        } catch (e) {
          return cb(e);
        }
      }

      return cb(null, queryResults);
    }
  });
};
