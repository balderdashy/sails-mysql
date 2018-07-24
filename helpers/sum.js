//  ███████╗██╗   ██╗███╗   ███╗     █████╗  ██████╗████████╗██╗ ██████╗ ███╗   ██╗
//  ██╔════╝██║   ██║████╗ ████║    ██╔══██╗██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
//  ███████╗██║   ██║██╔████╔██║    ███████║██║        ██║   ██║██║   ██║██╔██╗ ██║
//  ╚════██║██║   ██║██║╚██╔╝██║    ██╔══██║██║        ██║   ██║██║   ██║██║╚██╗██║
//  ███████║╚██████╔╝██║ ╚═╝ ██║    ██║  ██║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
//  ╚══════╝ ╚═════╝ ╚═╝     ╚═╝    ╚═╝  ╚═╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
//

module.exports = require('machine').build({


  friendlyName: 'SUM',


  description: 'Return the SUM of the records matched by the query.',


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
      description: 'A valid stage three Waterline query.',
      required: true,
      example: '==='
    }

  },


  exits: {

    success: {
      description: 'The results of the sum query.',
      outputType: 'ref'
    },

    invalidDatastore: {
      description: 'The datastore used is invalid. It is missing key pieces.'
    },

    badConnection: {
      friendlyName: 'Bad connection',
      description: 'A connection either could not be obtained or there was an error using the connection.'
    }

  },


  fn: function sum(inputs, exits) {
    // Dependencies
    var _ = require('@sailshq/lodash');
    var Converter = require('waterline-utils').query.converter;
    var Helpers = require('./private');


    // Store the Query input for easier access
    var query = inputs.query;
    query.meta = query.meta || {};


    // Find the model definition
    var model = inputs.models[query.using];
    if (!model) {
      return exits.invalidDatastore();
    }


    // Set a flag if a leased connection from outside the adapter was used or not.
    var leased = _.has(query.meta, 'leasedConnection');


    //  ╔═╗╔═╗╔╗╔╦  ╦╔═╗╦═╗╔╦╗  ┌┬┐┌─┐  ┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐
    //  ║  ║ ║║║║╚╗╔╝║╣ ╠╦╝ ║    │ │ │  └─┐ │ ├─┤ │ ├┤ │││├┤ │││ │
    //  ╚═╝╚═╝╝╚╝ ╚╝ ╚═╝╩╚═ ╩    ┴ └─┘  └─┘ ┴ ┴ ┴ ┴ └─┘┴ ┴└─┘┘└┘ ┴
    // Convert the Waterline criteria into a Waterline Query Statement. This
    // turns it into something that is declarative and can be easily used to
    // build a SQL query.
    // See: https://github.com/treelinehq/waterline-query-docs for more info
    // on Waterline Query Statements.
    var statement;
    try {
      statement = Converter({
        model: query.using,
        method: 'sum',
        criteria: query.criteria,
        values: query.numericAttrName
      });
    } catch (e) {
      return exits.error(e);
    }

    // Compile the original Waterline Query
    var compiledQuery;
    try {
      compiledQuery = Helpers.query.compileStatement(statement);
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
    Helpers.connection.spawnOrLeaseConnection(inputs.datastore, query.meta, function spawnConnectionCb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }

      //  ╦═╗╦ ╦╔╗╔  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  ╠╦╝║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
      //  ╩╚═╚═╝╝╚╝  └─┘└└─┘└─┘┴└─ ┴
      var queryType = 'sum';

      Helpers.query.runQuery({
        connection: connection,
        nativeQuery: compiledQuery.nativeQuery,
        valuesToEscape: compiledQuery.valuesToEscape,
        meta: compiledQuery.meta,
        queryType: queryType,
        disconnectOnError: leased ? false : true
      },

      function runQueryCb(err, report) {
        // The runQuery helper will automatically release the connection on error
        // if needed.
        if (err) {
          return exits.error(err);
        }

        // Always release the connection unless a leased connection from outside
        // the adapter was used.
        Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
          return exits.success(report.result);
        }); // </ releaseConnection >
      }); // </ runQuery >
    }); // </ spawnConnection >
  }
});
