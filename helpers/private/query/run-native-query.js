//  ██████╗ ██╗   ██╗███╗   ██╗    ███╗   ██╗ █████╗ ████████╗██╗██╗   ██╗███████╗
//  ██╔══██╗██║   ██║████╗  ██║    ████╗  ██║██╔══██╗╚══██╔══╝██║██║   ██║██╔════╝
//  ██████╔╝██║   ██║██╔██╗ ██║    ██╔██╗ ██║███████║   ██║   ██║██║   ██║█████╗
//  ██╔══██╗██║   ██║██║╚██╗██║    ██║╚██╗██║██╔══██║   ██║   ██║╚██╗ ██╔╝██╔══╝
//  ██║  ██║╚██████╔╝██║ ╚████║    ██║ ╚████║██║  ██║   ██║   ██║ ╚████╔╝ ███████╗
//  ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝    ╚═╝  ╚═══╝╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═══╝  ╚══════╝
//
//   ██████╗ ██╗   ██╗███████╗██████╗ ██╗   ██╗
//  ██╔═══██╗██║   ██║██╔════╝██╔══██╗╚██╗ ██╔╝
//  ██║   ██║██║   ██║█████╗  ██████╔╝ ╚████╔╝
//  ██║▄▄ ██║██║   ██║██╔══╝  ██╔══██╗  ╚██╔╝
//  ╚██████╔╝╚██████╔╝███████╗██║  ██║   ██║
//   ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝
//
// Run a native SQL query on an open connection and return the raw results.

var _ = require('@sailshq/lodash');
var MySQL = require('machinepack-mysql');

module.exports = function runNativeQuery(connection, query, valuesToEscape, meta, cb) {
  MySQL.sendNativeQuery({
    connection: connection,
    nativeQuery: query,
    valuesToEscape: valuesToEscape,
    meta: meta
  })
  .switch({
    error: function error(err) {
      return cb(err);
    },

    // If the query failed, try and parse it into a normalized format.
    queryFailed: function queryFailed(report) {
      // Parse the native query error into a normalized format
      var parsedError;
      try {
        parsedError = MySQL.parseNativeQueryError({
          nativeQueryError: report.error
        }).execSync();
      } catch (e) {
        return cb(e);
      }

      // If the catch all error was used, return an error instance instead of
      // the footprint.
      var catchAllError = false;

      if (parsedError.footprint.identity === 'catchall') {
        catchAllError = true;
      }

      if (catchAllError) {
        return cb(report.error);
      }

      // Attach parsed error as footprint on the native query error
      if (!_.has(report.error, 'footprint')) {
        report.error.footprint = parsedError;
      }

      return cb(report.error);
    },
    success: function success(report) {
      return cb(null, report.result.rows);
    }
  });
};
