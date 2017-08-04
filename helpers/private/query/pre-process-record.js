//  ██████╗ ██████╗ ███████╗    ██████╗ ██████╗  ██████╗  ██████╗███████╗███████╗███████╗
//  ██╔══██╗██╔══██╗██╔════╝    ██╔══██╗██╔══██╗██╔═══██╗██╔════╝██╔════╝██╔════╝██╔════╝
//  ██████╔╝██████╔╝█████╗█████╗██████╔╝██████╔╝██║   ██║██║     █████╗  ███████╗███████╗
//  ██╔═══╝ ██╔══██╗██╔══╝╚════╝██╔═══╝ ██╔══██╗██║   ██║██║     ██╔══╝  ╚════██║╚════██║
//  ██║     ██║  ██║███████╗    ██║     ██║  ██║╚██████╔╝╚██████╗███████╗███████║███████║
//  ╚═╝     ╚═╝  ╚═╝╚══════╝    ╚═╝     ╚═╝  ╚═╝ ╚═════╝  ╚═════╝╚══════╝╚══════╝╚══════╝
//
//  ██████╗ ███████╗ ██████╗ ██████╗ ██████╗ ██████╗  SSSSSS
//  ██╔══██╗██╔════╝██╔════╝██╔═══██╗██╔══██╗██╔══██╗ S
//  ██████╔╝█████╗  ██║     ██║   ██║██████╔╝██║  ██║ SSSSSS
//  ██╔══██╗██╔══╝  ██║     ██║   ██║██╔══██╗██║  ██║      S
//  ██║  ██║███████╗╚██████╗╚██████╔╝██║  ██║██████╔╝      S
//  ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚═════╝ ╚═╝  ╚═╝╚═════╝ SSSSSSS
//

var _ = require('@sailshq/lodash');
var utils = require('waterline-utils');
var eachRecordDeep = utils.eachRecordDeep;



/**
 * [exports description]
 *
 * TODO: Document this utility
 *
 * TODO: change the name of this utility to reflect the fact that its job is
 * to pre-process new incoming records (plural)
 *
 * @param  {[type]} options [description]
 * @return {[type]}         [description]
 */
module.exports = function preProcessRecord(options) {
  //  ╦  ╦╔═╗╦  ╦╔╦╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┌┬┐┬┌─┐┌┐┌┌─┐
  //  ╚╗╔╝╠═╣║  ║ ║║╠═╣ ║ ║╣   │ │├─┘ │ ││ ││││└─┐
  //   ╚╝ ╩ ╩╩═╝╩═╩╝╩ ╩ ╩ ╚═╝  └─┘┴   ┴ ┴└─┘┘└┘└─┘
  if (_.isUndefined(options) || !_.isPlainObject(options)) {
    throw new Error('Invalid options argument. Options must contain: records, identity, and orm.');
  }

  if (!_.has(options, 'records') || !_.isArray(options.records)) {
    throw new Error('Invalid option used in options argument. Missing or invalid records.');
  }

  if (!_.has(options, 'identity') || !_.isString(options.identity)) {
    throw new Error('Invalid option used in options argument. Missing or invalid identity.');
  }

  if (!_.has(options, 'orm') || !_.isPlainObject(options.orm)) {
    throw new Error('Invalid option used in options argument. Missing or invalid orm.');
  }

  // Key the collections by identity instead of column name
  var collections = _.reduce(options.orm.collections, function(memo, val) {
    memo[val.identity] = val;
    return memo;
  }, {});

  options.orm.collections = collections;

  // Run all the new, incoming records through the iterator so that they can be normalized
  // with anything adapter-specific before getting written to the database.
  // > (This should *never* go more than one level deep!)
  eachRecordDeep(options.records, function iterator(record, WLModel, depth) {
    if (depth !== 1) {
      throw new Error('Consistency violation: Incoming new records in a s3q should never necessitate deep iteration!  If you are seeing this error, it is probably because of a bug in this adapter, or in Waterline core.');
    }

    _.each(WLModel.definition, function checkAttributes(attrDef) {
      var columnName = attrDef.columnName;

      // JSON stringify the values provided for any `type: 'json'` attributes
      // because MySQL can't store JSON.
      if (attrDef.type === 'json' && _.has(record, columnName)) {

        // Special case: If this is the `null` literal, leave it alone.
        // But otherwise, stringify it into a JSON string.
        // (even if it's already a string!)
        if (!_.isNull(record[columnName])) {
          record[columnName] = JSON.stringify(record[columnName]);
        }

      }//>-

    });
  }, true, options.identity, options.orm);
};
