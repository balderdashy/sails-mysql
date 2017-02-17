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

  // Run all the new, incoming records through the iterator so that they can be normalized
  // with anything adapter-specific before getting written to the database.
  eachRecordDeep(options.records, function iterator(record, WLModel) {
    _.each(WLModel.definition, function checkAttributes(attrDef, attrName) {

      // JSON stringify the values provided for any `type: 'json'` attributes
      // because MySQL can't store JSON.
      if (attrDef.type === 'json' && _.has(record, attrName)) {

        // Special case: If this is the `null` literal, leave it alone.
        // But otherwise, stringify it into a JSON string.
        // (even if it's already a string!)
        if (!_.isNull(record[attrName])) {
          record[attrName] = JSON.stringify(record[attrName]);
        }

      }//>-

      // If the attribute is type ref and not a Buffer then don't allow it.
      if (attrDef.type === 'ref' && _.has(record, attrName) && !_.isNull(record[attrName])) {
        var isBuffer = record[attrName] instanceof Buffer;
        if (!isBuffer) {
          throw new Error('One of the values being set has an attribute type of `ref` but the value is not a Buffer. This adapter only accepts buffers for type `ref`. If you would like to store other types of data perhaps use type `json`.');
        }
      }
    });
  }, true, options.identity, options.orm);
};
