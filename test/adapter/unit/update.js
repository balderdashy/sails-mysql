var assert = require('assert');
var _ = require('@sailshq/lodash');
var Adapter = require('../../../lib/adapter');
var Support = require('../../support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Update', function() {
    // Test Setup
    before(function(done) {
      Support.Setup('test_update', function(err) {
        if (err) {
          return done(err);
        }

        // Seed the database with two simple records.
        Support.Seed('test_update', done);
      });
    });

    after(function(done) {
      Support.Teardown('test_update', done);
    });

    it('should update the correct record', function(done) {
      var query = {
        using: 'test_update',
        criteria: {
          where: {
            fieldA: 'foo'
          }
        },
        valuesToSet: {
          fieldA: 'foobar'
        },
        meta: {
          fetch: true
        }
      };

      Adapter.update('test', query, function(err, results) {
        if (err) {
          return done(err);
        }

        assert(_.isArray(results));
        assert.equal(results.length, 1);
        assert.equal(_.first(results).fieldA, 'foobar');
        assert.equal(_.first(results).fieldB, 'bar');

        return done();
      });
    });

    it('should be case in-sensitive', function(done) {
      var query = {
        using: 'test_update',
        criteria: {
          where: {
            fieldB: 'bAr_2'
          }
        },
        valuesToSet: {
          fieldA: 'FooBar'
        },
        meta: {
          fetch: true
        }
      };

      Adapter.update('test', query, function(err, results) {
        if (err) {
          return done(err);
        }

        assert(_.isArray(results));
        assert.equal(results.length, 1);
        assert.equal(_.first(results).fieldA, 'FooBar');
        assert.equal(_.first(results).fieldB, 'bAr_2');

        return done();
      });
    });

    // Look into the bowels of the PG Driver and ensure the Create function handles
    // it's connections properly.
    it('should release it\'s connection when completed', function(done) {
      var manager = Adapter.datastores.test.manager;
      var preConnectionsAvailable = manager.pool._allConnections.length;

      var query = {
        using: 'test_update',
        criteria: {},
        valuesToSet: {}
      };

      Adapter.update('test', query, function(err) {
        if (err) {
          return done(err);
        }

        var postConnectionsAvailable = manager.pool._allConnections.length;
        assert.equal(preConnectionsAvailable, postConnectionsAvailable);

        return done();
      });
    });
  });
});
