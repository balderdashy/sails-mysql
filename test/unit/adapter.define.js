var _ = require('lodash'),
    adapter = require('../../lib/adapter'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.registerConnection(['test_define', 'user'], done);
  });

  after(function(done) {
    support.Teardown('test_define', done);
  });

  // Attributes for the test table
  var definition = {
    id    : {
      type: 'integer',
      size: 64,
      autoIncrement: true
    },
    name  : {
      type: 'string',
      notNull: true
    },
    email : 'string',
    title : {
      type: 'string',
      defaultsTo: 'N/A'
    },
    phone : 'string',
    type  : 'string',
    favoriteFruit : {
      defaultsTo: 'blueberry',
      type: 'string'
    },
    age   : {
      type:'integer',
      defaultsTo: 18
    },
    vehicles: {
      type: 'json',
      defaultsTo: [ { make: 'Toyota', model: 'Corolla' } ]
    }
  };

  /**
   * DEFINE
   *
   * Create a new table with a defined set of attributes
   */

  describe('.define()', function() {

    describe('basic usage', function() {

      // Build Table from attributes
      it('should build the table', function(done) {

        adapter.define('test', 'test_define', definition, function(err) {
          adapter.describe('test', 'test_define', function(err, result) {
            Object.keys(result).length.should.eql(_.keys(definition).length);
            done();
          });
        });

      });

      // notNull constraint
      it('should create a bigint primary key', function(done) {
        adapter.define('test', 'test_define', definition, function(err) {
          support.Client(function(err, client) {
            var query = "SELECT COLUMN_TYPE from information_schema.COLUMNS "+
              "WHERE TABLE_SCHEMA = '" + support.Config.database + "' AND TABLE_NAME = 'test_define' AND COLUMN_NAME = 'id'";

            client.query(query, function(err, rows) {
              rows[0].COLUMN_TYPE.should.eql("bigint(20)");
              client.end();
              done();
            });
          });
        });
      });

      it('should define default string value in database', function(done) {
        adapter.define('test', 'test_define', definition, function(err) {
          support.Client(function(err, client) {
            var query = "SELECT COLUMN_TYPE, COLUMN_DEFAULT from information_schema.COLUMNS "+
              "WHERE TABLE_SCHEMA = '" + support.Config.database + "' AND TABLE_NAME = 'test_define' AND COLUMN_NAME = 'title'";

            client.query(query, function(err, rows) {
              rows[0].COLUMN_TYPE.should.eql('varchar(255)');
              rows[0].COLUMN_DEFAULT.should.eql('N/A');
              client.end();
              done();
            });
          });
        });
      });

      it('should define default integer value in database', function(done) {
        adapter.define('test', 'test_define', definition, function(err) {
          support.Client(function(err, client) {
            var query = "SELECT COLUMN_TYPE, COLUMN_DEFAULT from information_schema.COLUMNS "+
              "WHERE TABLE_SCHEMA = '" + support.Config.database + "' AND TABLE_NAME = 'test_define' AND COLUMN_NAME = 'age'";

            client.query(query, function(err, rows) {
              rows[0].COLUMN_TYPE.should.eql('int(11)');
              rows[0].COLUMN_DEFAULT.should.eql('18');
              client.end();
              done();
            });
          });
        });
      });

      it('should not define default value for text/blob column', function(done) {
        adapter.define('test', 'test_define', definition, function(err) {
          support.Client(function(err, client) {
            var query = "SELECT COLUMN_TYPE, COLUMN_DEFAULT from information_schema.COLUMNS "+
              "WHERE TABLE_SCHEMA = '" + support.Config.database + "' AND TABLE_NAME = 'test_define' AND COLUMN_NAME = 'vehicles'";

            client.query(query, function(err, rows) {
              rows[0].COLUMN_TYPE.should.eql('longtext');
              (rows[0].COLUMN_DEFAULT === null).should.be.true;
              client.end();
              done();
            });
          });
        });
      });
    });

    it('should add a notNull constraint', function(done) {
        adapter.define('test', 'test_define', definition, function(err) {
          support.Client(function(err, client) {
            var query = "SELECT IS_NULLABLE from information_schema.COLUMNS "+
              "WHERE TABLE_SCHEMA = '" + support.Config.database + "' AND TABLE_NAME = 'test_define' AND COLUMN_NAME = 'name'";

            client.query(query, function(err, rows) {
              rows[0].IS_NULLABLE.should.eql("NO");
              client.end();
              done();
            });
          });
        });
      });

    describe('reserved words', function() {

      after(function(done) {
        support.Client(function(err, client) {
          var query = 'DROP TABLE user;';
          client.query(query, function(err) {

            // close client
            client.end();

            done();
          });
        });
      });

      // Build Table from attributes
      it('should escape reserved words', function(done) {

        adapter.define('test', 'user', definition, function(err) {
          adapter.describe('test', 'user', function(err, result) {
            Object.keys(result).length.should.eql(_.keys(definition).length);
            done();
          });
        });

      });

    });

  });
});
