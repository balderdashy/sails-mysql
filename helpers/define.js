//  ██████╗ ███████╗███████╗██╗███╗   ██╗███████╗
//  ██╔══██╗██╔════╝██╔════╝██║████╗  ██║██╔════╝
//  ██║  ██║█████╗  █████╗  ██║██╔██╗ ██║█████╗
//  ██║  ██║██╔══╝  ██╔══╝  ██║██║╚██╗██║██╔══╝
//  ██████╔╝███████╗██║     ██║██║ ╚████║███████╗
//  ╚═════╝ ╚══════╝╚═╝     ╚═╝╚═╝  ╚═══╝╚══════╝
//

module.exports = require('machine').build({


  friendlyName: 'Define',


  description: 'Create a new table in the database based on a given schema.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      example: '==='
    },

    tableName: {
      description: 'The name of the table to describe.',
      required: true,
      example: 'users'
    },

    definition: {
      description: 'The definition of the schema to build.',
      required: true,
      example: {}
    },

    meta: {
      friendlyName: 'Meta (custom)',
      description: 'Additional stuff to pass to the driver.',
      extendedDescription: 'This is reserved for custom driver-specific extensions.',
      example: '==='
    }

  },


  exits: {

    success: {
      description: 'The table was created successfully.'
    },

    badConnection: {
      friendlyName: 'Bad connection',
      description: 'A connection either could not be obtained or there was an error using the connection.'
    }

  },


  fn: function define(inputs, exits) {
    // Dependencies
    var _ = require('@sailshq/lodash');
    var Helpers = require('./private');


    // Set a flag if a leased connection from outside the adapter was used or not.
    var leased = _.has(inputs.meta, 'leasedConnection');


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    // Spawn a new connection for running queries on.
    Helpers.connection.spawnOrLeaseConnection(inputs.datastore, inputs.meta, function spawnConnectionCb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }


      // Escape Table Name
      var tableName;
      try {
        tableName = Helpers.schema.escapeTableName(inputs.tableName);
      } catch (e) {
        // If there was an issue, release the connection
        Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
          return exits.error(e);
        });
        return;
      }


      //  ╔╗ ╦ ╦╦╦  ╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬  ┌─┐┌┬┐┬─┐┬┌┐┌┌─┐
      //  ╠╩╗║ ║║║   ║║  │─┼┐│ │├┤ ├┬┘└┬┘  └─┐ │ ├┬┘│││││ ┬
      //  ╚═╝╚═╝╩╩═╝═╩╝  └─┘└└─┘└─┘┴└─ ┴   └─┘ ┴ ┴└─┴┘└┘└─┘

      // Iterate through each attribute, building a query string
      var schema;
      try {
        schema = Helpers.schema.buildSchema(inputs.definition);
      } catch (e) {
        // If there was an issue, release the connection
        Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
          return exits.error(e);
        });
        return;
      }

      // Build Query
      var query = 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + schema + ')';


      //  ╦═╗╦ ╦╔╗╔  ┌─┐┬─┐┌─┐┌─┐┌┬┐┌─┐  ┌┬┐┌─┐┌┐ ┬  ┌─┐
      //  ╠╦╝║ ║║║║  │  ├┬┘├┤ ├─┤ │ ├┤    │ ├─┤├┴┐│  ├┤
      //  ╩╚═╚═╝╝╚╝  └─┘┴└─└─┘┴ ┴ ┴ └─┘   ┴ ┴ ┴└─┘┴─┘└─┘
      //  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  │─┼┐│ │├┤ ├┬┘└┬┘
      //  └─┘└└─┘└─┘┴└─ ┴
      Helpers.query.runNativeQuery(connection, query, [], undefined, function runNativeQueryCb(err) {
        if (err) {
          // If there was an issue, release the connection
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
            return exits.error(err);
          });
          return;
        }


        //  ╔╗ ╦ ╦╦╦  ╔╦╗  ┬┌┐┌┌┬┐┌─┐─┐ ┬┌─┐┌─┐
        //  ╠╩╗║ ║║║   ║║  ││││ ││├┤ ┌┴┬┘├┤ └─┐
        //  ╚═╝╚═╝╩╩═╝═╩╝  ┴┘└┘─┴┘└─┘┴ └─└─┘└─┘
        // Build any indexes
        Helpers.schema.buildIndexes({
          connection: connection,
          definition: inputs.definition,
          tableName: inputs.tableName
        },

        function buildIndexesCb(err) {
          Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
            if (err) {
              return exits.error(err);
            }

            return exits.success();
          });
          return;
        }); // </ buildIndexes() >
      }); // </ runNativeQuery >
    }); // </ spawnConnection >
  }
});
