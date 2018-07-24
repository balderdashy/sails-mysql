//  ██████╗ ███████╗███████╗████████╗██████╗  ██████╗ ██╗   ██╗
//  ██╔══██╗██╔════╝██╔════╝╚══██╔══╝██╔══██╗██╔═══██╗╚██╗ ██╔╝
//  ██║  ██║█████╗  ███████╗   ██║   ██████╔╝██║   ██║ ╚████╔╝
//  ██║  ██║██╔══╝  ╚════██║   ██║   ██╔══██╗██║   ██║  ╚██╔╝
//  ██████╔╝███████╗███████║   ██║   ██║  ██║╚██████╔╝   ██║
//  ╚═════╝ ╚══════╝╚══════╝   ╚═╝   ╚═╝  ╚═╝ ╚═════╝    ╚═╝
//
// Destroy the record(s) and return the values that were destroyed if needed.
// If a fetch was performed, first the records need to be searched for with the
// primary key selected.

var _ = require('@sailshq/lodash');
var runQuery = require('./run-query');
var compileStatement = require('./compile-statement');


module.exports = function insertRecord(options, cb) {
  //  ╦  ╦╔═╗╦  ╦╔╦╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┌┬┐┬┌─┐┌┐┌┌─┐
  //  ╚╗╔╝╠═╣║  ║ ║║╠═╣ ║ ║╣   │ │├─┘ │ ││ ││││└─┐
  //   ╚╝ ╩ ╩╩═╝╩═╩╝╩ ╩ ╩ ╚═╝  └─┘┴   ┴ ┴└─┘┘└┘└─┘
  if (_.isUndefined(options) || !_.isPlainObject(options)) {
    throw new Error('Invalid options argument. Options must contain: connection, statement, fetch, and primaryKey.');
  }

  if (!_.has(options, 'connection') || !_.isObject(options.connection)) {
    throw new Error('Invalid option used in options argument. Missing or invalid connection.');
  }

  if (!_.has(options, 'statement') || !_.isPlainObject(options.statement)) {
    throw new Error('Invalid option used in options argument. Missing or invalid statement.');
  }

  if (!_.has(options, 'primaryKey') || !_.isString(options.primaryKey)) {
    throw new Error('Invalid option used in options argument. Missing or invalid primaryKey.');
  }

  if (!_.has(options, 'fetch') || !_.isBoolean(options.fetch)) {
    throw new Error('Invalid option used in options argument. Missing or invalid fetch flag.');
  }


  //  ╔═╗╔═╗╔╦╗  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐  ┌┐ ┌─┐┬┌┐┌┌─┐  ┌┬┐┌─┐┌─┐┌┬┐┬─┐┌─┐┬ ┬┌─┐┌┬┐
  //  ║ ╦║╣  ║   ├┬┘├┤ │  │ │├┬┘ ││└─┐  ├┴┐├┤ │││││ ┬   ││├┤ └─┐ │ ├┬┘│ │└┬┘├┤  ││
  //  ╚═╝╚═╝ ╩   ┴└─└─┘└─┘└─┘┴└──┴┘└─┘  └─┘└─┘┴┘└┘└─┘  ─┴┘└─┘└─┘ ┴ ┴└─└─┘ ┴ └─┘─┴┘
  // If a fetch is used, the records that will be destroyed need to be found first.
  // This is because in order to (semi) accurately return the records that were
  // destroyed in MySQL first they need to be found, then destroyed.
  (function getRecordsToDestroy(proceed) {
    // Only look up the records if fetch was used
    if (!options.fetch) {
      return proceed();
    }

    // Otherwise build up a select query
    var fetchStatement = {
      from: options.statement.from,
      where: options.statement.where
    };

    //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║  ║ ║║║║╠═╝║║  ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴
    // Compile the statement into a native query.
    var compiledFetchQuery;
    try {
      compiledFetchQuery = compileStatement(fetchStatement);
    } catch (e) {
      // If the statement could not be compiled, return an error.
      return proceed(e);
    }

    //  ╦═╗╦ ╦╔╗╔  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠╦╝║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩╚═╚═╝╝╚╝  └─┘└└─┘└─┘┴└─ ┴
    // Run the initial find query
    runQuery({
      connection: options.connection,
      nativeQuery: compiledFetchQuery.nativeQuery,
      valuesToEscape: compiledFetchQuery.valuesToEscape,
      meta: compiledFetchQuery.meta,
      disconnectOnError: false,
      queryType: 'select'
    },

    function runQueryCb(err, report) {
      if (err) {
        return proceed(err);
      }

      return proceed(undefined, report);
    });
  })(function afterInitialFetchCb(err, selectReport) {
    if (err) {
      return cb(err);
    }

    //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║  ║ ║║║║╠═╝║║  ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴
    // Compile the destroy statement into a native query.
    var compiledUpdateQuery;
    try {
      compiledUpdateQuery = compileStatement(options.statement);
    } catch (e) {
      // If the statement could not be compiled, return an error.
      return cb(e);
    }

    //  ╦═╗╦ ╦╔╗╔  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠╦╝║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩╚═╚═╝╝╚╝  └─┘└└─┘└─┘┴└─ ┴
    // Run the destroy query
    runQuery({
      connection: options.connection,
      nativeQuery: compiledUpdateQuery.nativeQuery,
      valuesToEscape: compiledUpdateQuery.valuesToEscape,
      meta: compiledUpdateQuery.meta,
      disconnectOnError: false,
      queryType: 'destroy'
    },

    function runQueryCb(err, report) {
      if (err) {
        return cb(err);
      }

      // If no fetch was used, then nothing else needs to be done.
      if (!options.fetch) {
        return cb(undefined, report.result);
      }

      // Otherwise, return the selected records
      return cb(undefined, selectReport.result);
    });
  });
};
