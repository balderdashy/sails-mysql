//   █████╗ ██████╗ ██████╗      █████╗ ████████╗████████╗██████╗ ██╗██████╗ ██╗   ██╗████████╗███████╗
//  ██╔══██╗██╔══██╗██╔══██╗    ██╔══██╗╚══██╔══╝╚══██╔══╝██╔══██╗██║██╔══██╗██║   ██║╚══██╔══╝██╔════╝
//  ███████║██║  ██║██║  ██║    ███████║   ██║      ██║   ██████╔╝██║██████╔╝██║   ██║   ██║   █████╗
//  ██╔══██║██║  ██║██║  ██║    ██╔══██║   ██║      ██║   ██╔══██╗██║██╔══██╗██║   ██║   ██║   ██╔══╝
//  ██║  ██║██████╔╝██████╔╝    ██║  ██║   ██║      ██║   ██║  ██║██║██████╔╝╚██████╔╝   ██║   ███████╗
//  ╚═╝  ╚═╝╚═════╝ ╚═════╝     ╚═╝  ╚═╝   ╚═╝      ╚═╝   ╚═╝  ╚═╝╚═╝╚═════╝  ╚═════╝    ╚═╝   ╚══════╝
//

module.exports = require('machine').build({


  friendlyName: 'Add Attribute',


  description: 'Add an attribute to an existing table.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      readOnly: true,
      example: '==='
    },

    tableName: {
      description: 'The name of the table to create.',
      required: true,
      example: 'users'
    },

    definition: {
      description: 'The definition of the attribute to add.',
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
      description: 'The attribute was created successfully.'
    },

    badConfiguration: {
      description: 'The configuration was invalid.'
    },

    invalidDatastore: {
      description: 'The datastore used is invalid. It is missing key pieces.'
    }

  },


  fn: function addAttribute(inputs, exits) {
    // Dependencies
    var _ = require('@sailshq/lodash');
    var Helpers = require('./private');


    // Set a flag if a leased connection from outside the adapter was used or not.
    var leased = _.has(inputs.meta, 'leasedConnection');


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    // Spawn a new connection to run the queries on.
    Helpers.connection.spawnOrLeaseConnection(inputs.datastore, inputs.meta, function spawnConnectionCb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }


      //  ╔═╗╔═╗╔═╗╔═╗╔═╗╔═╗  ┌┬┐┌─┐┌┐ ┬  ┌─┐  ┌┐┌┌─┐┌┬┐┌─┐
      //  ║╣ ╚═╗║  ╠═╣╠═╝║╣    │ ├─┤├┴┐│  ├┤   │││├─┤│││├┤
      //  ╚═╝╚═╝╚═╝╩ ╩╩  ╚═╝   ┴ ┴ ┴└─┘┴─┘└─┘  ┘└┘┴ ┴┴ ┴└─┘
      var tableName;
      try {
        tableName = Helpers.schema.escapeTableName(inputs.tableName);
      } catch (e) {
        // If there was an error escaping the table name, release the connection
        // and return out the badConfiguration exit
        Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
          return exits.badConfiguration(e);
        });

        return;
      }

      // Iterate through each attribute, building the column parts of the query string
      var schema;
      try {
        schema = Helpers.schema.buildSchema(inputs.definition);
      } catch (e) {
        // If there was an error escaping the table name, release the connection
        // and return out the error exit
        Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
          return exits.error(e);
        });

        return;
      }

      // Build Query
      var query = 'ALTER TABLE ' + tableName + ' ADD COLUMN ' + schema;

      //  ╦═╗╦ ╦╔╗╔  ┌┐┌┌─┐┌┬┐┬┬  ┬┌─┐  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  ╠╦╝║ ║║║║  │││├─┤ │ │└┐┌┘├┤   │─┼┐│ │├┤ ├┬┘└┬┘
      //  ╩╚═╚═╝╝╚╝  ┘└┘┴ ┴ ┴ ┴ └┘ └─┘  └─┘└└─┘└─┘┴└─ ┴
      Helpers.query.runNativeQuery(connection, query, function cb(err) {
        // Always release the connection no matter what the error state.
        Helpers.connection.releaseConnection(connection, leased, function cb() {
          // If the native query had an error, return that error
          if (err) {
            return exits.error(err);
          }

          return exits.success();
        });
      });
    });
  }
});
