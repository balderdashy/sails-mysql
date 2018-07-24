//  ██╗███╗   ██╗██╗████████╗██╗ █████╗ ██╗     ██╗███████╗███████╗
//  ██║████╗  ██║██║╚══██╔══╝██║██╔══██╗██║     ██║╚══███╔╝██╔════╝
//  ██║██╔██╗ ██║██║   ██║   ██║███████║██║     ██║  ███╔╝ █████╗
//  ██║██║╚██╗██║██║   ██║   ██║██╔══██║██║     ██║ ███╔╝  ██╔══╝
//  ██║██║ ╚████║██║   ██║   ██║██║  ██║███████╗██║███████╗███████╗
//  ╚═╝╚═╝  ╚═══╝╚═╝   ╚═╝   ╚═╝╚═╝  ╚═╝╚══════╝╚═╝╚══════╝╚══════╝
//
//   ██████╗ ██╗   ██╗███████╗██████╗ ██╗   ██╗     ██████╗ █████╗  ██████╗██╗  ██╗███████╗
//  ██╔═══██╗██║   ██║██╔════╝██╔══██╗╚██╗ ██╔╝    ██╔════╝██╔══██╗██╔════╝██║  ██║██╔════╝
//  ██║   ██║██║   ██║█████╗  ██████╔╝ ╚████╔╝     ██║     ███████║██║     ███████║█████╗
//  ██║▄▄ ██║██║   ██║██╔══╝  ██╔══██╗  ╚██╔╝      ██║     ██╔══██║██║     ██╔══██║██╔══╝
//  ╚██████╔╝╚██████╔╝███████╗██║  ██║   ██║       ╚██████╗██║  ██║╚██████╗██║  ██║███████╗
//   ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝        ╚═════╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚══════╝
//
// Builds up a query cache for use when native joins are performed. The purpose
// of this is because in some cases a query can't be fulfilled in a single query.
// The Query Cache is responsible for holding intermediate values until all of
// the operations are completed. The records can then be nested together and
// returned as a single array of nested values.

var _ = require('@sailshq/lodash');
var utils = require('waterline-utils');

module.exports = function initializeQueryCache(options) {
  //  ╦  ╦╔═╗╦  ╦╔╦╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┌┬┐┬┌─┐┌┐┌┌─┐
  //  ╚╗╔╝╠═╣║  ║ ║║╠═╣ ║ ║╣   │ │├─┘ │ ││ ││││└─┐
  //   ╚╝ ╩ ╩╩═╝╩═╩╝╩ ╩ ╩ ╚═╝  └─┘┴   ┴ ┴└─┘┘└┘└─┘
  if (_.isUndefined(options) || !_.isPlainObject(options)) {
    throw new Error('Invalid options argument. Options must contain: connection, query, model, schemaName, and tableName.');
  }

  if (!_.has(options, 'instructions') || !_.isPlainObject(options.instructions)) {
    throw new Error('Invalid option used in options argument. Missing or invalid instructions.');
  }

  if (!_.has(options, 'models') || !_.isPlainObject(options.models)) {
    throw new Error('Invalid option used in options argument. Missing or invalid models.');
  }

  if (!_.has(options, 'sortedResults') || !_.isPlainObject(options.sortedResults)) {
    throw new Error('Invalid option used in options argument. Missing or invalid sortedResults.');
  }


  //  ╔╗ ╦ ╦╦╦  ╔╦╗  ┌┐┌┌─┐┬ ┬  ┌─┐┌─┐┌─┐┬ ┬┌─┐
  //  ╠╩╗║ ║║║   ║║  │││├┤ │││  │  ├─┤│  ├─┤├┤
  //  ╚═╝╚═╝╩╩═╝═╩╝  ┘└┘└─┘└┴┘  └─┘┴ ┴└─┘┴ ┴└─┘
  // Build up a new cache to use to hold query results
  var queryCache = utils.joins.queryCache();


  //  ╔═╗╦═╗╔═╗╔═╗╔═╗╔═╗╔═╗  ┌─┐┌─┐┌─┐┬ ┬┌─┐  ┬  ┬┌─┐┬  ┬ ┬┌─┐┌─┐
  //  ╠═╝╠╦╝║ ║║  ║╣ ╚═╗╚═╗  │  ├─┤│  ├─┤├┤   └┐┌┘├─┤│  │ │├┤ └─┐
  //  ╩  ╩╚═╚═╝╚═╝╚═╝╚═╝╚═╝  └─┘┴ ┴└─┘┴ ┴└─┘   └┘ ┴ ┴┴─┘└─┘└─┘└─┘
  _.each(options.instructions, function processInstruction(val, key) {
    // Grab the instructions that define a particular join set
    var popInstructions = val.instructions;

    // Grab the strategy used for the join
    var strategy = val.strategy.strategy;

    // Find the Primary Key of the parent used in the join
    var model = options.models[_.first(popInstructions).parent];
    if (!model) {
      throw new Error('Invalid parent table name used when caching query results. Perhaps the join criteria is invalid?');
    }

    var pkAttr = model.primaryKey;
    var pkColumnName = model.definition[pkAttr].columnName || pkAttr;

    // Build an alias to use for the association. The alias is the name of the
    // assocation defined by the user. It's created in a model whenever a
    // model or collection is defined.
    var alias;

    // Hold an optional keyName to use in strategy 1. This represents the
    // foreign key value on the parent that will be replaced by the populated
    // value.
    var keyName;

    // If the join strategy is a hasFk strategy this means the parent contains
    // the value being populated - i.e. populating a model record. Therefore
    // the keyName is the name of the attribute on the parent record.
    if (val.strategy && val.strategy.strategy === 1) {
      alias = _.first(popInstructions).alias;
      keyName = _.first(popInstructions).parentKey;

    // Otherwise this must be a collection populating so just grab the alias
    // directly off the instructions.
    } else {
      alias = _.first(popInstructions).alias;
    }


    // Process each of the parents and build up a local cache containing
    // values for the populated children.
    _.each(options.sortedResults.parents, function buildAliasCache(parentRecord) {
      var cache = {
        attrName: key,
        parentPkAttr: pkColumnName,
        belongsToPkValue: parentRecord[pkColumnName],
        keyName: keyName || alias,
        type: strategy
      };

      // Grab the join keys used in the query
      var childKey = _.first(popInstructions).childKey;
      var parentKey = _.first(popInstructions).parentKey;

      // Find any records in the children that match up to the join keys
      var records = _.filter(options.sortedResults.children[alias], function findChildren(child) {
        // If this is a VIA_JUNCTOR join, use the foreign key we built up,
        // otherwise check equality between child and parent join keys.
        if (strategy === 3) {
          return child._parent_fk === parentRecord[parentKey];
        }

        return child[childKey] === parentRecord[parentKey];
      });

      // If this is a many-to-many strategy, be sure to clear the foreign
      // key value that was added as part of the join process. The end user
      // doesn't care about that.
      if (strategy === 3) {
        _.each(records, function cleanRecords(record) {
          delete record._parent_fk;
        });
      }

      // Store the child on the cache
      if (records.length) {
        cache.records = records;
      }

      // Store the local cache value in the query cache
      queryCache.set(cache);
    }); // </ buildAliasCache >
  }); // </ processInstructions >

  // Return the QueryCache
  return queryCache;
};
