//  ██████╗ ███████╗███████╗ ██████╗██████╗ ██╗██████╗ ███████╗
//  ██╔══██╗██╔════╝██╔════╝██╔════╝██╔══██╗██║██╔══██╗██╔════╝
//  ██║  ██║█████╗  ███████╗██║     ██████╔╝██║██████╔╝█████╗
//  ██║  ██║██╔══╝  ╚════██║██║     ██╔══██╗██║██╔══██╗██╔══╝
//  ██████╔╝███████╗███████║╚██████╗██║  ██║██║██████╔╝███████╗
//  ╚═════╝ ╚══════╝╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝╚═════╝ ╚══════╝
//

module.exports = require('machine').build({


  friendlyName: 'Describe',


  description: 'Describe a table in the related data store.',


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

    meta: {
      friendlyName: 'Meta (custom)',
      description: 'Additional stuff to pass to the driver.',
      extendedDescription: 'This is reserved for custom driver-specific extensions.',
      example: '==='
    }

  },


  exits: {

    success: {
      description: 'The results of the describe query.',
      outputVariableName: 'records',
      outputType: 'ref'
    },

    badConnection: {
      friendlyName: 'Bad connection',
      description: 'A connection either could not be obtained or there was an error using the connection.'
    }

  },


  fn: function describe(inputs, exits) {
    // Dependencies
    var _ = require('@sailshq/lodash');
    var Helpers = require('./private');

    // Set a flag if a leased connection from outside the adapter was used or not.
    var leased = _.has(inputs.meta, 'leasedConnection');


    //   ██████╗ ██╗   ██╗███████╗██████╗ ██╗███████╗███████╗
    //  ██╔═══██╗██║   ██║██╔════╝██╔══██╗██║██╔════╝██╔════╝
    //  ██║   ██║██║   ██║█████╗  ██████╔╝██║█████╗  ███████╗
    //  ██║▄▄ ██║██║   ██║██╔══╝  ██╔══██╗██║██╔══╝  ╚════██║
    //  ╚██████╔╝╚██████╔╝███████╗██║  ██║██║███████╗███████║
    //   ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝╚══════╝╚══════╝
    //
    // These native queries are responsible for describing a single table and the
    // various attributes that make them.

    var describeQuery = 'DESCRIBE ' + inputs.tableName;
    var autoIncrementQuery = 'SHOW INDEX FROM ' + inputs.tableName;


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    // Spawn a new connection to run the queries on.
    Helpers.connection.spawnOrLeaseConnection(inputs.datastore, inputs.meta, function spawnConnectionCb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }


      //  ╦═╗╦ ╦╔╗╔  ┌┬┐┌─┐┌─┐┌─┐┬─┐┬┌┐ ┌─┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  ╠╦╝║ ║║║║   ││├┤ └─┐│  ├┬┘│├┴┐├┤   │─┼┐│ │├┤ ├┬┘└┬┘
      //  ╩╚═╚═╝╝╚╝  ─┴┘└─┘└─┘└─┘┴└─┴└─┘└─┘  └─┘└└─┘└─┘┴└─ ┴
      Helpers.query.runNativeQuery(connection, describeQuery, [], undefined, function runDescribeQueryCb(err, describeResults) {
        if (err) {
          // Release the connection on error
          Helpers.connection.releaseConnection(connection, leased, function cb() {
            // If the table doesn't exist, return an empty object
            if (err.code === 'ER_NO_SUCH_TABLE') {
              return exits.success({ schema: {} });
            }

            return exits.error(err);
          });
          return;
        }


        //  ╦═╗╦ ╦╔╗╔  ┌─┐┬ ┬┌┬┐┌─┐   ┬┌┐┌┌─┐┬─┐┌─┐┌┬┐┌─┐┌┐┌┌┬┐
        //  ╠╦╝║ ║║║║  ├─┤│ │ │ │ │───│││││  ├┬┘├┤ │││├┤ │││ │
        //  ╩╚═╚═╝╝╚╝  ┴ ┴└─┘ ┴ └─┘   ┴┘└┘└─┘┴└─└─┘┴ ┴└─┘┘└┘ ┴
        //  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
        //  │─┼┐│ │├┤ ├┬┘└┬┘
        //  └─┘└└─┘└─┘┴└─ ┴
        Helpers.query.runNativeQuery(connection, autoIncrementQuery, [], undefined, function runAutoIncrementQueryCb(err, incrementResults) {
          if (err) {
            // Release the connection on error
            Helpers.connection.releaseConnection(connection, leased, function cb() {
              return exits.error(err);
            });
            return;
          }


          //  ╔═╗╦═╗╔═╗╔═╗╔═╗╔═╗╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
          //  ╠═╝╠╦╝║ ║║  ║╣ ╚═╗╚═╗  │─┼┐│ │├┤ ├┬┘└┬┘
          //  ╩  ╩╚═╚═╝╚═╝╚═╝╚═╝╚═╝  └─┘└└─┘└─┘┴└─ ┴
          //  ┬─┐┌─┐┌─┐┬ ┬┬ ┌┬┐┌─┐
          //  ├┬┘├┤ └─┐│ ││  │ └─┐
          //  ┴└─└─┘└─┘└─┘┴─┘┴ └─┘

          // Normalize Schema
          var schema = {};
          _.each(describeResults, function normalize(column) {
            // Set Type
            schema[column.Field] = {
              // Remove (n) column-size indicators
              type: column.Type.replace(/\([0-9]+\)$/, '')
            };

            // Check for primary key
            if (column.Key === 'PRI') {
              schema[column.Field].primaryKey = true;
            }

            // Check for uniqueness
            if (column.Key === 'UNI') {
              schema[column.Field].unique = true;
            }

            // If also an integer set auto increment attribute
            if (column.Type === 'int(11)') {
              schema[column.Field].autoIncrement = true;
            }

            // Loop Through Indexes and Add Properties
            _.each(incrementResults, function processIndexes(result) {
              _.each(schema, function loopThroughSchema(attr) {
                if (attr.Field !== result.Column_name) {
                  return;
                }

                attr.indexed = true;
              });
            });
          });

          Helpers.connection.releaseConnection(connection, leased, function cb() {
            // Return the model schema
            return exits.success({ schema: schema });
          }); // </ releaseConnection >
        }); // </ runAutoIncrementQuery >
      }); // </ runDescribeQuery >
    }); // </ spawnConnection >
  }
});
