var assert = require('assert');
var _ = require('@sailshq/lodash');
var Adapter = require('../../../lib/adapter');
var Support = require('../../support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Define', function() {
    // Test Setup
    before(function(done) {
      Support.registerConnection(['test_define'], done);
    });

    after(function(done) {
      Support.Teardown('test_define', done);
    });

    // Attributes for the test table
    var definition = {
      id: {
        columnType: 'serial',
        autoIncrement: true
      },
      name: {
        columnType: 'text',
        notNull: true
      },
      email: {
        columnType: 'text'
      },
      title: {
        columnType: 'text'
      },
      phone: {
        columnType: 'text'
      },
      type: {
        columnType: 'text'
      },
      favoriteFruit: {
        columnType: 'text'
      },
      age: {
        columnType: 'integer'
      }
    };

    it('should create a table in the database', function(done) {
      Adapter.define('test', 'test_define', definition, function(err) {
        if (err) {
          return done(err);
        }

        Adapter.describe('test', 'test_define', function(err, result) {
          if (err) {
            return done(err);
          }

          assert(_.isPlainObject(result));

          assert.equal(_.keys(result).length, 8);
          assert(result.id);
          assert(result.name);
          assert(result.email);
          assert(result.title);
          assert(result.phone);
          assert(result.type);
          assert(result.favoriteFruit);
          assert(result.age);

          return done();
        });
      });
    });
  });
});
