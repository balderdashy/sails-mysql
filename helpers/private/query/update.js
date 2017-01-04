//  ██╗   ██╗██████╗ ██████╗  █████╗ ████████╗███████╗
//  ██║   ██║██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝██╔════╝
//  ██║   ██║██████╔╝██║  ██║███████║   ██║   █████╗
//  ██║   ██║██╔═══╝ ██║  ██║██╔══██║   ██║   ██╔══╝
//  ╚██████╔╝██║     ██████╔╝██║  ██║   ██║   ███████╗
//   ╚═════╝ ╚═╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝
//
// Modify the record(s) and return the values that were modified if needed.

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

  if (!_.has(options, 'fetch') || !_.isBoolean(options.fetch)) {
    throw new Error('Invalid option used in options argument. Missing or invalid fetch flag.');
  }


  //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
  //  ║  ║ ║║║║╠═╝║║  ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
  //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴
  // Compile the statement into a native query.
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
  // Run the initial query
  runQuery({
    connection: options.connection,
    nativeQuery: compiledUpdateQuery,
    disconnectOnError: false,
    queryType: 'update'
  },

  function runQueryCb(err, report) {
    if (err) {
      return cb(err);
    }

    // If no fetch was used, then nothing else needs to be done.
    if (!options.fetch) {
      return cb(undefined, report.result);
    }

    //  ╔═╗╔═╗╦═╗╔═╗╔═╗╦═╗╔╦╗  ┌┬┐┬ ┬┌─┐  ┌─┐┌─┐┌┬┐┌─┐┬ ┬
    //  ╠═╝║╣ ╠╦╝╠╣ ║ ║╠╦╝║║║   │ ├─┤├┤   ├┤ ├┤  │ │  ├─┤
    //  ╩  ╚═╝╩╚═╚  ╚═╝╩╚═╩ ╩   ┴ ┴ ┴└─┘  └  └─┘ ┴ └─┘┴ ┴
    // Otherwise, fetch the newly inserted record
    var fetchStatement = {
      select: '*',
      from: options.statement.using,
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
      return cb(err);
    }


    //  ╦═╗╦ ╦╔╗╔  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠╦╝║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩╚═╚═╝╝╚╝  └─┘└└─┘└─┘┴└─ ┴
    // Run the fetch query.
    runQuery({
      connection: options.connection,
      nativeQuery: compiledFetchQuery,
      disconnectOnError: false,
      queryType: 'select'
    }, function runQueryCb(err, report) {
      if (err) {
        return cb(err);
      }

      return cb(undefined, report.result);
    });
  });
};
