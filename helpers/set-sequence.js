//  ███████╗███████╗████████╗    ███████╗███████╗ ██████╗ ██╗   ██╗███████╗███╗   ██╗ ██████╗███████╗
//  ██╔════╝██╔════╝╚══██╔══╝    ██╔════╝██╔════╝██╔═══██╗██║   ██║██╔════╝████╗  ██║██╔════╝██╔════╝
//  ███████╗█████╗     ██║       ███████╗█████╗  ██║   ██║██║   ██║█████╗  ██╔██╗ ██║██║     █████╗
//  ╚════██║██╔══╝     ██║       ╚════██║██╔══╝  ██║▄▄ ██║██║   ██║██╔══╝  ██║╚██╗██║██║     ██╔══╝
//  ███████║███████╗   ██║       ███████║███████╗╚██████╔╝╚██████╔╝███████╗██║ ╚████║╚██████╗███████╗
//  ╚══════╝╚══════╝   ╚═╝       ╚══════╝╚══════╝ ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═══╝ ╚═════╝╚══════╝
//

module.exports = require('machine').build({


  friendlyName: 'Set Sequence',


  description: 'Sets the current version of a sequence from a migration.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      readOnly: true,
      example: '==='
    },

    sequenceName: {
      description: 'The name of the sequence to set the value for.',
      required: true,
      example: 'user_id_seq'
    },

    sequenceValue: {
      description: 'The value to set the sequence to.',
      required: true,
      example: 123
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
      description: 'The sequence was set successfully.'
    },

    badConnection: {
      friendlyName: 'Bad connection',
      description: 'A connection either could not be obtained or there was an error using the connection.'
    }

  },


  fn: function select(inputs, exits) {
    // Dependencies
    var _ = require('@sailshq/lodash');
    var Helpers = require('./private');

    // Set a flag if a leased connection from outside the adapter was used or not.
    var leased = _.has(inputs.meta, 'leasedConnection');


    //  ╔═╗╔═╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  ╚═╗╠═╝╠═╣║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //  ╚═╝╩  ╩ ╩╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    //  ┌─┐┬─┐  ┬ ┬┌─┐┌─┐  ┬  ┌─┐┌─┐┌─┐┌─┐┌┬┐  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //  │ │├┬┘  │ │└─┐├┤   │  ├┤ ├─┤└─┐├┤  ││  │  │ │││││││├┤ │   │ ││ ││││
    //  └─┘┴└─  └─┘└─┘└─┘  ┴─┘└─┘┴ ┴└─┘└─┘─┴┘  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    // Spawn a new connection for running queries on.
    Helpers.connection.spawnOrLeaseConnection(inputs.datastore, inputs.meta, function spawnConnectionCb(err, connection) {
      if (err) {
        return exits.badConnection(err);
      }

      var sequenceQuery = 'SELECT setval(' + '\'' + schemaName + '.' + inputs.sequenceName + '\', ' + inputs.sequenceValue + ', true)';

      // Run Sequence Query
      Helpers.query.runQuery({
        connection: connection,
        nativeQuery: sequenceQuery,
        disconnectOnError: false
      }, function runQueryCb(err) {
        // Always release the connection unless a leased connection from outside
        // the adapter was used.
        Helpers.connection.releaseConnection(connection, leased, function releaseConnectionCb() {
          if (err) {
            return exits.error(err);
          }

          return exits.success();
        }); // </ releaseConnection >
      }); // </ runQuery >
    }); // </ spawnOrLeaseConnection >
  }
});
