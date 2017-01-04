//  ██████╗ ███████╗███╗   ███╗ ██████╗ ██╗   ██╗███████╗     █████╗ ████████╗████████╗██████╗ ██╗██████╗ ██╗   ██╗████████╗███████╗
//  ██╔══██╗██╔════╝████╗ ████║██╔═══██╗██║   ██║██╔════╝    ██╔══██╗╚══██╔══╝╚══██╔══╝██╔══██╗██║██╔══██╗██║   ██║╚══██╔══╝██╔════╝
//  ██████╔╝█████╗  ██╔████╔██║██║   ██║██║   ██║█████╗      ███████║   ██║      ██║   ██████╔╝██║██████╔╝██║   ██║   ██║   █████╗
//  ██╔══██╗██╔══╝  ██║╚██╔╝██║██║   ██║╚██╗ ██╔╝██╔══╝      ██╔══██║   ██║      ██║   ██╔══██╗██║██╔══██╗██║   ██║   ██║   ██╔══╝
//  ██║  ██║███████╗██║ ╚═╝ ██║╚██████╔╝ ╚████╔╝ ███████╗    ██║  ██║   ██║      ██║   ██║  ██║██║██████╔╝╚██████╔╝   ██║   ███████╗
//  ╚═╝  ╚═╝╚══════╝╚═╝     ╚═╝ ╚═════╝   ╚═══╝  ╚══════╝    ╚═╝  ╚═╝   ╚═╝      ╚═╝   ╚═╝  ╚═╝╚═╝╚═════╝  ╚═════╝    ╚═╝   ╚══════╝
//

module.exports = require('machine').build({


  friendlyName: 'Remove Attribute',


  description: 'Remove an attribute from an existing table.',


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

    attributeName: {
      description: 'The name of the attribute to remove.',
      required: true,
      example: 'name'
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
      description: 'The attribute was removed successfully.'
    },

    badConfiguration: {
      description: 'The configuration was invalid.'
    }

  },


  fn: function removeAttribute(inputs, exits) {
    // Dependencies
    var _ = require('@sailshq/lodash');
    var Helpers = require('./private');


    // Set a flag if a leased connection from outside the adapter was used or not.
    var leased = _.has(inputs.meta, 'leasedConnection');


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
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
          return exits.error(new Error('There was an issue escaping the table name ' + inputs.tableName + '.\n\n' + e.stack));
        });
        return;
      }

      // Build Query
      var query = 'ALTER TABLE ' + tableName + ' DROP COLUMN ' + inputs.attributeName + ' RESTRICT';


      //  ╦═╗╦ ╦╔╗╔  ┌─┐┬ ┌┬┐┌─┐┬─┐  ┌┬┐┌─┐┌┐ ┬  ┌─┐
      //  ╠╦╝║ ║║║║  ├─┤│  │ ├┤ ├┬┘   │ ├─┤├┴┐│  ├┤
      //  ╩╚═╚═╝╝╚╝  ┴ ┴┴─┘┴ └─┘┴└─   ┴ ┴ ┴└─┘┴─┘└─┘
      //  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
      //  │─┼┐│ │├┤ ├┬┘└┬┘
      //  └─┘└└─┘└─┘┴└─ ┴
      Helpers.query.runNativeQuery(connection, query, function runNativeQueryCb(err) {
        // Always release the connection back into the pool
        Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
          if (err) {
            return exits.error(err);
          }

          return exits.success();
        }); // </ releaseConnection >
      }); // </ runNativeQuery >
    }); // </ spawnConnection >
  }
});
