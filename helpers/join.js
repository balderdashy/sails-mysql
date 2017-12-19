//       ██╗ ██████╗ ██╗███╗   ██╗
//       ██║██╔═══██╗██║████╗  ██║
//       ██║██║   ██║██║██╔██╗ ██║
//  ██   ██║██║   ██║██║██║╚██╗██║
//  ╚█████╔╝╚██████╔╝██║██║ ╚████║
//   ╚════╝  ╚═════╝ ╚═╝╚═╝  ╚═══╝
//
module.exports = require('machine').build({


  friendlyName: 'Join',


  description: 'Support native joins on the database.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      readOnly: true,
      example: '==='
    },

    models: {
      description: 'An object containing all of the model definitions that have been registered.',
      required: true,
      example: '==='
    },

    query: {
      description: 'A normalized Waterline Stage Three Query.',
      required: true,
      example: '==='
    }

  },


  exits: {

    success: {
      description: 'The query was run successfully.',
      outputType: 'ref'
    },

    badConnection: {
      friendlyName: 'Bad connection',
      description: 'A connection either could not be obtained or there was an error using the connection.'
    }

  },


  fn: function join(inputs, exits) {
    var _ = require('@sailshq/lodash');
    var async = require('async');
    var WLUtils = require('waterline-utils');
    var Helpers = require('./private');

    var meta = _.has(inputs.query, 'meta') ? inputs.query.meta : {};

    // Set a flag if a leased connection from outside the adapter was used or not.
    var leased = _.has(meta, 'leasedConnection');


    //  ╔═╗╦╔╗╔╔╦╗  ┌┬┐┌─┐┌┐ ┬  ┌─┐  ┌─┐┬─┐┬┌┬┐┌─┐┬─┐┬ ┬  ┬┌─┌─┐┬ ┬
    //  ╠╣ ║║║║ ║║   │ ├─┤├┴┐│  ├┤   ├─┘├┬┘││││├─┤├┬┘└┬┘  ├┴┐├┤ └┬┘
    //  ╚  ╩╝╚╝═╩╝   ┴ ┴ ┴└─┘┴─┘└─┘  ┴  ┴└─┴┴ ┴┴ ┴┴└─ ┴   ┴ ┴└─┘ ┴
    // Find the model definition
    var model = inputs.models[inputs.query.using];
    if (!model) {
      return exits.invalidDatastore();
    }

    // Grab the primary key attribute for the main table name
    var primaryKeyAttr = model.primaryKey;
    var primaryKeyColumnName = model.definition[primaryKeyAttr].columnName || primaryKeyAttr;

    // Build a fake ORM and process the records.
    var orm = {
      collections: inputs.models
    };

    //  ╔╗ ╦ ╦╦╦  ╔╦╗  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐┌─┐
    //  ╠╩╗║ ║║║   ║║  └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │ └─┐
    //  ╚═╝╚═╝╩╩═╝═╩╝  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴ └─┘
    // Attempt to build up the statements necessary for the query.
    var statements;
    try {
      statements = WLUtils.joins.convertJoinCriteria({
        query: inputs.query,
        getPk: function getPk(tableName) {
          var model = inputs.models[tableName];
          if (!model) {
            throw new Error('Invalid parent table name used when caching query results. Perhaps the join criteria is invalid?');
          }

          var pkAttrName = model.primaryKey;
          var pkColumnName = model.definition[pkAttrName].columnName || pkAttrName;

          return pkColumnName;
        }
      });
    } catch (e) {
      return exits.error(e);
    }


    //  ╔═╗╔═╗╔╗╔╦  ╦╔═╗╦═╗╔╦╗  ┌─┐┌─┐┬─┐┌─┐┌┐┌┌┬┐
    //  ║  ║ ║║║║╚╗╔╝║╣ ╠╦╝ ║   ├─┘├─┤├┬┘├┤ │││ │
    //  ╚═╝╚═╝╝╚╝ ╚╝ ╚═╝╩╚═ ╩   ┴  ┴ ┴┴└─└─┘┘└┘ ┴
    //  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐
    //  └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │
    //  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴
    // Convert the parent statement into a native query. If the query can be run
    // in a single query then this will be the only query that runs.
    var compiledQuery;
    try {
      compiledQuery = Helpers.query.compileStatement(statements.parentStatement);
    } catch (e) {
      return exits.error(e);
    }


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    //  ┌─┐┬─┐  ┬ ┬┌─┐┌─┐  ┬  ┌─┐┌─┐┌─┐┌─┐┌┬┐  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  │ │├┬┘  │ │└─┐├┤   │  ├┤ ├─┤└─┐├┤  ││  │  │ │││││││├┤ │   │ ││ ││││
    //  └─┘┴└─  └─┘└─┘└─┘  ┴─┘└─┘┴ ┴└─┘└─┘─┴┘  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    // Spawn a new connection for running queries on.
    Helpers.connection.spawnOrLeaseConnection(inputs.datastore, meta, function spawnCb(err, connection) {
      if (err) {
        return exits.error(err);
      }


      //  ╦═╗╦ ╦╔╗╔  ┌┬┐┬ ┬┌─┐  ┌┐┌┌─┐┌┬┐┬┬  ┬┌─┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  ╠╦╝║ ║║║║   │ ├─┤├┤   │││├─┤ │ │└┐┌┘├┤   │─┼┐│ │├┤ ├┬┘└┬┘
      //  ╩╚═╚═╝╝╚╝   ┴ ┴ ┴└─┘  ┘└┘┴ ┴ ┴ ┴ └┘ └─┘  └─┘└└─┘└─┘┴└─ ┴
      Helpers.query.runNativeQuery(connection, compiledQuery.nativeQuery, compiledQuery.valuesToEscape, compiledQuery.meta, function parentQueryCb(err, parentResults) {
        if (err) {
          // Release the connection on error
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
            return exits.error(err);
          });
          return;
        }

        // If there weren't any joins being performed or no parent records were
        // returned, release the connection and return the results.
        if (!_.has(inputs.query, 'joins') || !parentResults.length) {
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb(err) {
            if (err) {
              return exits.error(err);
            }

            return exits.success(parentResults);
          });
          return;
        }


        //  ╔═╗╦╔╗╔╔╦╗  ┌─┐┬ ┬┬┬  ┌┬┐┬─┐┌─┐┌┐┌  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐
        //  ╠╣ ║║║║ ║║  │  ├─┤││   ││├┬┘├┤ │││  ├┬┘├┤ │  │ │├┬┘ ││└─┐
        //  ╚  ╩╝╚╝═╩╝  └─┘┴ ┴┴┴─┘─┴┘┴└─└─┘┘└┘  ┴└─└─┘└─┘└─┘┴└──┴┘└─┘
        // If there was a join that was either performed or still needs to be
        // performed, look into the results for any children records that may
        // have been joined and splt them out from the parent.
        var sortedResults;
        try {
          sortedResults = WLUtils.joins.detectChildrenRecords(primaryKeyColumnName, parentResults);
        } catch (e) {
          // Release the connection if there was an error.
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
            return exits.error(e);
          });
          return;
        }


        //  ╦╔╗╔╦╔╦╗╦╔═╗╦  ╦╔═╗╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬  ┌─┐┌─┐┌─┐┬ ┬┌─┐
        //  ║║║║║ ║ ║╠═╣║  ║╔═╝║╣   │─┼┐│ │├┤ ├┬┘└┬┘  │  ├─┤│  ├─┤├┤
        //  ╩╝╚╝╩ ╩ ╩╩ ╩╩═╝╩╚═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴   └─┘┴ ┴└─┘┴ ┴└─┘
        var queryCache;
        try {
          queryCache = Helpers.query.initializeQueryCache({
            instructions: statements.instructions,
            models: inputs.models,
            sortedResults: sortedResults
          });
        } catch (e) {
          // Release the connection if there was an error.
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
            return exits.error(e);
          });
          return;
        }


        //  ╔═╗╔╦╗╔═╗╦═╗╔═╗  ┌─┐┌─┐┬─┐┌─┐┌┐┌┌┬┐┌─┐
        //  ╚═╗ ║ ║ ║╠╦╝║╣   ├─┘├─┤├┬┘├┤ │││ │ └─┐
        //  ╚═╝ ╩ ╚═╝╩╚═╚═╝  ┴  ┴ ┴┴└─└─┘┘└┘ ┴ └─┘
        try {
          queryCache.setParents(sortedResults.parents);
        } catch (e) {
          // Release the connection if there was an error.
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
            return exits.error(e);
          });
          return;
        }


        //  ╔═╗╦ ╦╔═╗╔═╗╦╔═  ┌─┐┌─┐┬─┐  ┌─┐┬ ┬┬┬  ┌┬┐┬─┐┌─┐┌┐┌
        //  ║  ╠═╣║╣ ║  ╠╩╗  ├┤ │ │├┬┘  │  ├─┤││   ││├┬┘├┤ │││
        //  ╚═╝╩ ╩╚═╝╚═╝╩ ╩  └  └─┘┴└─  └─┘┴ ┴┴┴─┘─┴┘┴└─└─┘┘└┘
        //  ┌─┐ ┬ ┬┌─┐┬─┐┬┌─┐┌─┐
        //  │─┼┐│ │├┤ ├┬┘│├┤ └─┐
        //  └─┘└└─┘└─┘┴└─┴└─┘└─┘
        // Now that all the parents are found, check if there are any child
        // statements that need to be processed. If not, close the connection and
        // return the combined results.
        if (!statements.childStatements || !statements.childStatements.length) {
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb(err) {
            if (err) {
              return exits.error(err);
            }

            // Combine records in the cache to form nested results
            var combinedResults;
            try {
              combinedResults = queryCache.combineRecords();
            } catch (e) {
              return exits.error(e);
            }

            // Process each record to normalize output
            try {
              Helpers.query.processEachRecord({
                records: combinedResults,
                identity: model.identity,
                orm: orm
              });
            } catch (e) {
              return exits.error(e);
            }

            // Return the combined results
            exits.success(combinedResults);
          });
          return;
        }


        //  ╔═╗╔═╗╦  ╦  ╔═╗╔═╗╔╦╗  ┌─┐┌─┐┬─┐┌─┐┌┐┌┌┬┐
        //  ║  ║ ║║  ║  ║╣ ║   ║   ├─┘├─┤├┬┘├┤ │││ │
        //  ╚═╝╚═╝╩═╝╩═╝╚═╝╚═╝ ╩   ┴  ┴ ┴┴└─└─┘┘└┘ ┴
        //  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┌─┐
        //  ├┬┘├┤ │  │ │├┬┘ ││└─┐
        //  ┴└─└─┘└─┘└─┘┴└──┴┘└─┘
        // There is more work to be done now. Go through the parent records and
        // build up an array of the primary keys.
        var parentKeys = _.map(queryCache.getParents(), function pluckPk(record) {
          return record[primaryKeyColumnName];
        });


        //  ╔═╗╦═╗╔═╗╔═╗╔═╗╔═╗╔═╗  ┌─┐┬ ┬┬┬  ┌┬┐  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐┌─┐
        //  ╠═╝╠╦╝║ ║║  ║╣ ╚═╗╚═╗  │  ├─┤││   ││  └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │ └─┐
        //  ╩  ╩╚═╚═╝╚═╝╚═╝╚═╝╚═╝  └─┘┴ ┴┴┴─┘─┴┘  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴ └─┘
        // For each child statement, figure out how to turn the statement into
        // a native query and then run it. Add the results to the query cache.
        async.each(statements.childStatements, function processChildStatements(template, next) {
          //  ╦═╗╔═╗╔╗╔╔╦╗╔═╗╦═╗  ┬┌┐┌  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
          //  ╠╦╝║╣ ║║║ ║║║╣ ╠╦╝  ││││  │─┼┐│ │├┤ ├┬┘└┬┘
          //  ╩╚═╚═╝╝╚╝═╩╝╚═╝╩╚═  ┴┘└┘  └─┘└└─┘└─┘┴└─ ┴
          //  ┌┬┐┌─┐┌┬┐┌─┐┬  ┌─┐┌┬┐┌─┐
          //   │ ├┤ │││├─┘│  ├─┤ │ ├┤
          //   ┴ └─┘┴ ┴┴  ┴─┘┴ ┴ ┴ └─┘
          // If the statement is an IN query, replace the values with the parent
          // keys.
          if (template.queryType === 'in') {
            // Pull the last AND clause out - it's the one we added
            var inClause = _.pullAt(template.statement.where.and, template.statement.where.and.length - 1);

            // Grab the object inside the array that comes back
            inClause = _.first(inClause);

            // Modify the inClause using the actual parent key values
            _.each(inClause, function modifyInClause(val) {
              val.in = parentKeys;
            });

            // Reset the statement
            template.statement.where.and.push(inClause);
          }


          //  ╦═╗╔═╗╔╗╔╔╦╗╔═╗╦═╗  ┬ ┬┌┐┌┬┌─┐┌┐┌  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
          //  ╠╦╝║╣ ║║║ ║║║╣ ╠╦╝  │ ││││││ ││││  │─┼┐│ │├┤ ├┬┘└┬┘
          //  ╩╚═╚═╝╝╚╝═╩╝╚═╝╩╚═  └─┘┘└┘┴└─┘┘└┘  └─┘└└─┘└─┘┴└─ ┴
          //  ┌┬┐┌─┐┌┬┐┌─┐┬  ┌─┐┌┬┐┌─┐
          //   │ ├┤ │││├─┘│  ├─┤ │ ├┤
          //   ┴ └─┘┴ ┴┴  ┴─┘┴ ┴ ┴ └─┘
          // If the statement is a UNION type, loop through each parent key and
          // build up a proper query.
          if (template.queryType === 'union') {
            var unionStatements = [];

            // Build up an array of generated statements
            _.each(parentKeys, function buildUnion(parentPk) {
              var unionStatement = _.merge({}, template.statement);

              // Replace the placeholder `?` values with the primary key of the
              // parent record.
              var andClause = _.pullAt(unionStatement.where.and, unionStatement.where.and.length - 1);
              _.each(_.first(andClause), function replaceValue(val, key) {
                _.first(andClause)[key] = parentPk;
              });

              // Add the UNION statement to the array of other statements
              unionStatement.where.and.push(_.first(andClause));
              unionStatements.push(unionStatement);
            });

            // Replace the final statement with the UNION ALL clause
            if (unionStatements.length) {
              template.statement = { unionAll: unionStatements };
            }
          }

          // If there isn't a statement to be run, then just return
          if (!template.statement) {
            return next();
          }


          //  ╔═╗╔═╗╔╦╗╔═╗╦╦  ╔═╗  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐
          //  ║  ║ ║║║║╠═╝║║  ║╣   └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │
          //  ╚═╝╚═╝╩ ╩╩  ╩╩═╝╚═╝  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴
          // Attempt to convert the statement into a native query
          var compiledQuery;
          try {
            compiledQuery = Helpers.query.compileStatement(template.statement);
          } catch (e) {
            return next(e);
          }


          //  ╦═╗╦ ╦╔╗╔  ┌─┐┬ ┬┬┬  ┌┬┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
          //  ╠╦╝║ ║║║║  │  ├─┤││   ││  │─┼┐│ │├┤ ├┬┘└┬┘
          //  ╩╚═╚═╝╝╚╝  └─┘┴ ┴┴┴─┘─┴┘  └─┘└└─┘└─┘┴└─ ┴
          // Run the native query
          Helpers.query.runNativeQuery(connection, compiledQuery.nativeQuery, compiledQuery.valuesToEscape, compiledQuery.meta, function parentQueryCb(err, queryResults) {
            if (err) {
              return next(err);
            }

            // Extend the values in the cache to include the values from the
            // child query.
            queryCache.extend(queryResults, template.instructions);

            return next();
          });
        },

        function asyncEachCb(err) {
          // Always release the connection unless a leased connection from outside
          // the adapter was used.
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
            if (err) {
              return exits.error(err);
            }

            // Combine records in the cache to form nested results
            var combinedResults = queryCache.combineRecords();

            // Process each record to normalize output
            try {
              Helpers.query.processEachRecord({
                records: combinedResults,
                identity: model.identity,
                orm: orm
              });
            } catch (e) {
              return exits.error(e);
            }

            // Return the combined results
            return exits.success(combinedResults);
          }); // </ releaseConnection >
        }); // </ asyncEachCb >
      }); // </ runNativeQuery >
    }); // </ spawnConnection >
  }
});
