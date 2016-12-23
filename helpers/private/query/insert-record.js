//  ██╗███╗   ██╗███████╗███████╗██████╗ ████████╗
//  ██║████╗  ██║██╔════╝██╔════╝██╔══██╗╚══██╔══╝
//  ██║██╔██╗ ██║███████╗█████╗  ██████╔╝   ██║
//  ██║██║╚██╗██║╚════██║██╔══╝  ██╔══██╗   ██║
//  ██║██║ ╚████║███████║███████╗██║  ██║   ██║
//  ╚═╝╚═╝  ╚═══╝╚══════╝╚══════╝╚═╝  ╚═╝   ╚═╝
//
//  ██████╗ ███████╗ ██████╗ ██████╗ ██████╗ ██████╗
//  ██╔══██╗██╔════╝██╔════╝██╔═══██╗██╔══██╗██╔══██╗
//  ██████╔╝█████╗  ██║     ██║   ██║██████╔╝██║  ██║
//  ██╔══██╗██╔══╝  ██║     ██║   ██║██╔══██╗██║  ██║
//  ██║  ██║███████╗╚██████╗╚██████╔╝██║  ██║██████╔╝
//  ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚═════╝ ╚═╝  ╚═╝╚═════╝
//
// Insert the record and return the values that were inserted.

var _ = require('@sailshq/lodash');
var runQuery = require('./run-query');
var compileStatement = require('./compile-statement');
var releaseConnection = require('../connection/release-connection');


module.exports = function insertRecord(options, cb) {
  //  ╦  ╦╔═╗╦  ╦╔╦╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┌┬┐┬┌─┐┌┐┌┌─┐
  //  ╚╗╔╝╠═╣║  ║ ║║╠═╣ ║ ║╣   │ │├─┘ │ ││ ││││└─┐
  //   ╚╝ ╩ ╩╩═╝╩═╩╝╩ ╩ ╩ ╚═╝  └─┘┴   ┴ ┴└─┘┘└┘└─┘
  if (_.isUndefined(options) || !_.isPlainObject(options)) {
    throw new Error('Invalid options argument. Options must contain: connection, query, model, schemaName, leased, and tableName.');
  }

  if (!_.has(options, 'connection') || !_.isObject(options.connection)) {
    throw new Error('Invalid option used in options argument. Missing or invalid connection.');
  }

  if (!_.has(options, 'query') || (!_.isPlainObject(options.query) && !_.isString(options.query))) {
    throw new Error('Invalid option used in options argument. Missing or invalid query.');
  }

  if (!_.has(options, 'model') || !_.isPlainObject(options.model)) {
    throw new Error('Invalid option used in options argument. Missing or invalid model.');
  }

  if (!_.has(options, 'tableName') || !_.isString(options.tableName)) {
    throw new Error('Invalid option used in options argument. Missing or invalid tableName.');
  }

  if (!_.has(options, 'leased') || !_.isBoolean(options.leased)) {
    throw new Error('Invalid option used in options argument. Missing or invalid leased flag.');
  }


  //  ╦╔╗╔╔═╗╔═╗╦═╗╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
  //  ║║║║╚═╗║╣ ╠╦╝ ║   │─┼┐│ │├┤ ├┬┘└┬┘
  //  ╩╝╚╝╚═╝╚═╝╩╚═ ╩   └─┘└└─┘└─┘┴└─ ┴
  runQuery({
    connection: options.connection,
    nativeQuery: options.query,
    queryType: 'insert',
    disconnectOnError: false
  },

  function runQueryCb(err, insertReport) {
    // If the query failed to run, release the connection and return the parsed
    // error footprint.
    if (err) {
      releaseConnection(options.connection, options.leased, function releaseCb() {
        return cb(err);
      });

      return;
    }

    // Hold the results of the insert query
    var insertResults = insertReport.result;


    //  ╔═╗╦╔╗╔╔╦╗  ┬┌┐┌┌─┐┌─┐┬─┐┌┬┐┌─┐┌┬┐  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐
    //  ╠╣ ║║║║ ║║  ││││└─┐├┤ ├┬┘ │ ├┤  ││  ├┬┘├┤ │  │ │├┬┘ ││└─┐
    //  ╚  ╩╝╚╝═╩╝  ┴┘└┘└─┘└─┘┴└─ ┴ └─┘─┴┘  ┴└─└─┘└─┘└─┘┴└──┴┘└─┘

    // Find the Primary Key field for the model
    var pk;
    try {
      pk = options.model.primaryKey;
    } catch (e) {
      throw new Error('Could not determine a Primary Key for the model: ' + options.model.tableName + '.');
    }

    // Build up a criteria statement to run
    var criteriaStatement = {
      select: ['*'],
      from: options.tableName,
      where: {}
    };

    // Insert dynamic primary key value into query
    criteriaStatement.where[pk] = {
      in: insertResults.inserted
    };

    // Build an IN query from the results of the insert query
    var compiledReport;
    try {
      compiledReport = compileStatement(criteriaStatement);
    } catch (e) {
      return cb(e);
    }

    // Run the FIND query
    runQuery({
      connection: options.connection,
      nativeQuery: compiledReport,
      queryType: 'select',
      disconnectOnError: false
    },

    function runFindQueryCb(err, findReport) {
      // If the query failed to run, release the connection and return the parsed
      // error footprint.
      if (err) {
        releaseConnection(options.connection, options.leased, function releaseCb() {
          return cb(err);
        });

        return;
      }

      // Return the FIND results
      return cb(null, findReport.result);
    });
  });
};
