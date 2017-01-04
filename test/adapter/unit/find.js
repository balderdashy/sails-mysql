var assert = require('assert');
var _ = require('@sailshq/lodash');
var Adapter = require('../../../lib/adapter');
var Support = require('../../support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Find', function() {
    // Test Setup
    before(function(done) {
      Support.Setup('test_find', function(err) {
        if (err) {
          return done(err);
        }

        // Seed the database with two simple records.
        Support.Seed('test_find', done);
      });
    });

    after(function(done) {
      Support.Teardown('test_find', done);
    });


    it('should select the correct record', function(done) {
      var query = {
        using: 'test_find',
        criteria: {
          where: {
            fieldA: 'foo'
          }
        }
      };

      Adapter.find('test', query, function(err, results) {
        if (err) {
          return done(err);
        }

        assert(_.isArray(results));
        assert.equal(results.length, 1);
        assert.equal(_.first(results).fieldA, 'foo');
        assert.equal(_.first(results).fieldB, 'bar');

        return done();
      });
    });

    it('should return all the records', function(done) {
      var query = {
        using: 'test_find',
        criteria: {}
      };

      Adapter.find('test', query, function(err, results) {
        if (err) {
          return done(err);
        }

        assert(_.isArray(results));
        assert.equal(results.length, 2);

        return done();
      });
    });

    it('should be case sensitive', function(done) {
      var query = {
        using: 'test_find',
        criteria: {
          where: {
            fieldB: 'bAr_2'
          }
        }
      };

      Adapter.find('test', query, function(err, results) {
        if (err) {
          return done(err);
        }

        assert(_.isArray(results));
        assert.equal(results.length, 1);
        assert.equal(_.first(results).fieldA, 'foo_2');
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
        using: 'test_find',
        criteria: {}
      };

      Adapter.find('test', query, function(err) {
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
