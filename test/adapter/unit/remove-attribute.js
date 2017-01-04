var assert = require('assert');
var _ = require('@sailshq/lodash');
var Adapter = require('../../../lib/adapter');
var Support = require('../../support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Remove Attribute', function() {
    // Test Setup
    before(function(done) {
      Support.Setup('test_remove_attribute', done);
    });

    after(function(done) {
      Support.Teardown('test_remove_attribute', done);
    });

    it('should remove a column from a table', function(done) {
      Adapter.removeAttribute('test', 'test_remove_attribute', 'fieldB', function(err) {
        if (err) {
          return done(err);
        }

        Adapter.describe('test', 'test_remove_attribute', function(err, result) {
          if (err) {
            return done(err);
          }

          assert(_.isPlainObject(result));
          assert.equal(_.keys(result).length, 2);
          assert(!result.fieldB);

          return done();
        });
      });
    });
  });
});
