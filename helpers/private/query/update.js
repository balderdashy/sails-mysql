//  ██╗   ██╗██████╗ ██████╗  █████╗ ████████╗███████╗
//  ██║   ██║██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝██╔════╝
//  ██║   ██║██████╔╝██║  ██║███████║   ██║   █████╗
//  ██║   ██║██╔═══╝ ██║  ██║██╔══██║   ██║   ██╔══╝
//  ╚██████╔╝██║     ██████╔╝██║  ██║   ██║   ███████╗
//   ╚═════╝ ╚═╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝
//
// Modify the record(s) and return the values that were modified if needed.
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


  //  ╔═╗╔═╗╔╦╗  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐  ┌┐ ┌─┐┬┌┐┌┌─┐  ┬ ┬┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐
  //  ║ ╦║╣  ║   ├┬┘├┤ │  │ │├┬┘ ││└─┐  ├┴┐├┤ │││││ ┬  │ │├─┘ ││├─┤ │ ├┤  ││
  //  ╚═╝╚═╝ ╩   ┴└─└─┘└─┘└─┘┴└──┴┘└─┘  └─┘└─┘┴┘└┘└─┘  └─┘┴  ─┴┘┴ ┴ ┴ └─┘─┴┘
  // If a fetch is used, the records that will be updated need to be found first.
  // This is because in order to (semi) accurately return the records that were
  // updated in MySQL first they need to be found, then updated, then found again.
  // Why? Because if you have a criteria such as update name to foo where name = bar
  // Once the records have been updated there is no way to get them again. So first
  // select the primary keys of the records to update, update the records, and then
  // search for those records.
  (function getRecordsToUpdate(proceed) {
    // Only look up the records if fetch was used
    if (!options.fetch) {
      return proceed();
    }

    // Otherwise build up a select query
    var fetchStatement = {
      select: [options.primaryKey],
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
    // Compile the update statement into a native query.
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
      nativeQuery: compiledUpdateQuery.nativeQuery,
      valuesToEscape: compiledUpdateQuery.valuesToEscape,
      meta: compiledUpdateQuery.meta,
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
        where: {}
      };

      // Build the fetch statement where clause
      var selectPks = _.map(selectReport.result, function mapPks(record) {
        return record[options.primaryKey];
      });

      fetchStatement.where[options.primaryKey] = {
        in: selectPks
      };


      // Handle case where pk value was changed:
      if (!_.isUndefined(options.statement.update[options.primaryKey])) {
        // There should only ever be either zero or one record that were found before.
        if (selectPks.length === 0) { /* do nothing */ }
        else if (selectPks.length === 1) {
          var oldPkValue = selectPks[0];
          _.remove(fetchStatement.where[options.primaryKey].in, oldPkValue);
          var newPkValue = options.statement.update[options.primaryKey];
          fetchStatement.where[options.primaryKey].in.push(newPkValue);
        }
        else {
          return cb(new Error('Consistency violation: Updated multiple records to have the same primary key value. (PK values should be unique!)'));
        }
      }


      //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  ║  ║ ║║║║╠═╝║║  ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
      //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴
      // Compile the statement into a native query.
      var compiledFetchQuery;
      try {
        compiledFetchQuery = compileStatement(fetchStatement);
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
        nativeQuery: compiledFetchQuery.nativeQuery,
        valuesToEscape: compiledFetchQuery.valuesToEscape,
        meta: compiledFetchQuery.meta,
        disconnectOnError: false,
        queryType: 'select'
      }, function runQueryCb(err, report) {
        if (err) {
          return cb(err);
        }

        return cb(undefined, report.result);
      });
    });
  });
};
