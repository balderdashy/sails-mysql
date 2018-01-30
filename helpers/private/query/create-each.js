//   ██████╗██████╗ ███████╗ █████╗ ████████╗███████╗    ███████╗ █████╗  ██████╗██╗  ██╗
//  ██╔════╝██╔══██╗██╔════╝██╔══██╗╚══██╔══╝██╔════╝    ██╔════╝██╔══██╗██╔════╝██║  ██║
//  ██║     ██████╔╝█████╗  ███████║   ██║   █████╗      █████╗  ███████║██║     ███████║
//  ██║     ██╔══██╗██╔══╝  ██╔══██║   ██║   ██╔══╝      ██╔══╝  ██╔══██║██║     ██╔══██║
//  ╚██████╗██║  ██║███████╗██║  ██║   ██║   ███████╗    ███████╗██║  ██║╚██████╗██║  ██║
//   ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝    ╚══════╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝
//
// Run creates in order and return the records. This is needed because MySQL
// lacks the ability to return multiple insert id's from a bulk insert.
//
// So when a createEach call from Waterline is made with the `fetch: true` flag
// turned on, the records must be inserted one by one in order to return the
// correct primary keys.

var _ = require('@sailshq/lodash');
var async = require('async');
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


  //  ███╗   ██╗ ██████╗ ███╗   ██╗      ███████╗███████╗████████╗ ██████╗██╗  ██╗
  //  ████╗  ██║██╔═══██╗████╗  ██║      ██╔════╝██╔════╝╚══██╔══╝██╔════╝██║  ██║
  //  ██╔██╗ ██║██║   ██║██╔██╗ ██║█████╗█████╗  █████╗     ██║   ██║     ███████║
  //  ██║╚██╗██║██║   ██║██║╚██╗██║╚════╝██╔══╝  ██╔══╝     ██║   ██║     ██╔══██║
  //  ██║ ╚████║╚██████╔╝██║ ╚████║      ██║     ███████╗   ██║   ╚██████╗██║  ██║
  //  ╚═╝  ╚═══╝ ╚═════╝ ╚═╝  ╚═══╝      ╚═╝     ╚══════╝   ╚═╝    ╚═════╝╚═╝  ╚═╝
  //
  //   ██████╗██████╗ ███████╗ █████╗ ████████╗███████╗
  //  ██╔════╝██╔══██╗██╔════╝██╔══██╗╚══██╔══╝██╔════╝
  //  ██║     ██████╔╝█████╗  ███████║   ██║   █████╗
  //  ██║     ██╔══██╗██╔══╝  ██╔══██║   ██║   ██╔══╝
  //  ╚██████╗██║  ██║███████╗██║  ██║   ██║   ███████╗
  //   ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝
  //
  // If the fetch flag was used, then the statement will need to be broken up
  // into a series of async queries. Otherwise just run a bulk insert.
  if (!options.fetch) {
    //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║  ║ ║║║║╠═╝║║  ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴
    // Compile the statement into a native query.
    var compiledQuery;
    try {
      compiledQuery = compileStatement(options.statement, options.meta);
    } catch (e) {
      // If the statement could not be compiled, return an error.
      return cb(e);
    }

    //  ╦═╗╦ ╦╔╗╔  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠╦╝║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩╚═╚═╝╝╚╝  └─┘└└─┘└─┘┴└─ ┴
    // Run the initial query (bulk insert)
    runQuery({
      connection: options.connection,
      nativeQuery: compiledQuery.nativeQuery,
      valuesToEscape: compiledQuery.valuesToEscape,
      meta: compiledQuery.meta,
      disconnectOnError: false,
      queryType: 'insert'
    },

    function runQueryCb(err, report) {
      if (err) {
        return cb(err);
      }

      return cb(undefined, report.result);
    });

    // Return early
    return;
  }


  //  ███████╗███████╗████████╗ ██████╗██╗  ██╗     ██████╗██████╗ ███████╗ █████╗ ████████╗███████╗
  //  ██╔════╝██╔════╝╚══██╔══╝██╔════╝██║  ██║    ██╔════╝██╔══██╗██╔════╝██╔══██╗╚══██╔══╝██╔════╝
  //  █████╗  █████╗     ██║   ██║     ███████║    ██║     ██████╔╝█████╗  ███████║   ██║   █████╗
  //  ██╔══╝  ██╔══╝     ██║   ██║     ██╔══██║    ██║     ██╔══██╗██╔══╝  ██╔══██║   ██║   ██╔══╝
  //  ██║     ███████╗   ██║   ╚██████╗██║  ██║    ╚██████╗██║  ██║███████╗██║  ██║   ██║   ███████╗
  //  ╚═╝     ╚══════╝   ╚═╝    ╚═════╝╚═╝  ╚═╝     ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝
  //
  // Break apart the statement's insert records and create a single create query
  // for each one. Collect the result of the insertId's to be returned.
  var newRecords = options.statement.insert;
  var insertIds = [];

  // Be sure to run these in series so that the insert order is maintained.
  async.eachSeries(newRecords, function runCreateQuery(record, nextRecord) {
    // Build up a statement to use.
    var statement = {
      insert: record,
      into: options.statement.into
    };

    //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║  ║ ║║║║╠═╝║║  ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴
    // Compile the statement into a native query.
    var compiledQuery;
    try {
      compiledQuery = compileStatement(statement);
    } catch (e) {
      // If the statement could not be compiled, return an error.
      return nextRecord(e);
    }

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
    if (statement.insert[options.primaryKey]) {
      insertOptions.customPrimaryKey = statement.insert[options.primaryKey];
    }

    //  ╦═╗╦ ╦╔╗╔  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠╦╝║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩╚═╚═╝╝╚╝  └─┘└└─┘└─┘┴└─ ┴
    // Run the initial query (bulk insert)
    runQuery(insertOptions, function runQueryCb(err, report) {
      if (err) {
        return nextRecord(err);
      }

      // Add the insert id to the array
      insertIds.push(report.result.inserted);

      return nextRecord(undefined, report.result);
    });
  },

  function fetchCreateCb(err) {
    if (err) {
      return cb(err);
    }


    //  ╔═╗╔═╗╦═╗╔═╗╔═╗╦═╗╔╦╗  ┌┬┐┬ ┬┌─┐  ┌─┐┌─┐┌┬┐┌─┐┬ ┬
    //  ╠═╝║╣ ╠╦╝╠╣ ║ ║╠╦╝║║║   │ ├─┤├┤   ├┤ ├┤  │ │  ├─┤
    //  ╩  ╚═╝╩╚═╚  ╚═╝╩╚═╩ ╩   ┴ ┴ ┴└─┘  └  └─┘ ┴ └─┘┴ ┴
    var fetchStatement = {
      select: '*',
      from: options.statement.into,
      where: {},
      orderBy: [{}]
    };

    // Sort the records by primary key
    fetchStatement.orderBy[0][options.primaryKey] = 'ASC';

    // Build up the WHERE clause for the statement to get the newly inserted
    // records.
    fetchStatement.where[options.primaryKey] = { 'in': insertIds };


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
