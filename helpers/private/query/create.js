//   ██████╗██████╗ ███████╗ █████╗ ████████╗███████╗
//  ██╔════╝██╔══██╗██╔════╝██╔══██╗╚══██╔══╝██╔════╝
//  ██║     ██████╔╝█████╗  ███████║   ██║   █████╗
//  ██║     ██╔══██╗██╔══╝  ██╔══██║   ██║   ██╔══╝
//  ╚██████╗██║  ██║███████╗██║  ██║   ██║   ███████╗
//   ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝
//
// Perform a create query and fetch the record if needed.

var _ = require('@sailshq/lodash');
var compileStatement = require('./compile-statement');
var runQuery = require('./run-query');

module.exports = function createEach(options, cb) {
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

  if (!_.has(options, 'primaryKey') || !_.isString(options.primaryKey)) {
    throw new Error('Invalid option used in options argument. Missing or invalid primaryKey flag.');
  }


  //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
  //  ║  ║ ║║║║╠═╝║║  ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
  //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴
  // Compile the statement into a native query.
  var compiledQuery;
  try {
    compiledQuery = compileStatement(options.statement);
  } catch (e) {
    // If the statement could not be compiled, return an error.
    return cb(e);
  }

  //  ╦═╗╦ ╦╔╗╔  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
  //  ╠╦╝║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
  //  ╩╚═╚═╝╝╚╝  └─┘└└─┘└─┘┴└─ ┴
  // Run the initial query (bulk insert)

  var insertOptions = {
    connection: options.connection,
    nativeQuery: compiledQuery.nativeQuery,
    valuesToEscape: compiledQuery.valuesToEscape,
    meta: compiledQuery.meta,
    disconnectOnError: false,
    queryType: 'insert'
  };

  // Determine if a custom primary key value was used. If so pass it down so that
  // the report can be used correctly. MySQL doesn't return these values.
  if (options.statement.insert[options.primaryKey]) {
    insertOptions.customPrimaryKey = options.statement.insert[options.primaryKey];
  }


  runQuery(insertOptions, function runQueryCb(err, report) {
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
      from: options.statement.into,
      where: {}
    };

    // Build up the WHERE clause for the statement to get the newly inserted
    // records.
    fetchStatement.where[options.primaryKey] = report.result.inserted;


    //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║  ║ ║║║║╠═╝║║  ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴
    // Compile the statement into a native query.
    var compiledQuery;
    try {
      compiledQuery = compileStatement(fetchStatement);
    } catch (err) {
      // If the statement could not be compiled, return an error.
      return cb(err);
    }


    //  ╦═╗╦ ╦╔╗╔  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠╦╝║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩╚═╚═╝╝╚╝  └─┘└└─┘└─┘┴└─ ┴
    // Run the fetch query.
    runQuery({
      connection: options.connection,
      nativeQuery: compiledQuery.nativeQuery,
      valuesToEscape: compiledQuery.valuesToEscape,
      meta: compiledQuery.meta,
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
