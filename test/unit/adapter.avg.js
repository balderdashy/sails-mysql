var Sequel = require('waterline-sequel'),
    should = require('should'),
    Support = require('./support/bootstrap');

describe('query', function() {

  /**
   * AVG
   *
   * Adds a AVG select parameter to a sql statement
   */

  describe('.avg()', function() {

    describe('with array', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        average: ['age']
      };

      var schema = {'test': Support.Schema('test', { name: { type: 'text' }, age: { type: 'integer'} })};

      it('should use the AVG aggregate option in the select statement', function() {
        var query = new Sequel(schema, Support.SqlOptions).find('test', criteria);
        var sql = 'SELECT CAST( AVG("test"."age") AS float) AS age FROM "test" AS "test"  WHERE ' +
                  'LOWER("test"."name") = $1 ';

        query.query[0].should.eql(sql);
      });
    });

    describe('with string', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        average: 'age'
      };

      var schema = {'test': Support.Schema('test', { name: { type: 'text' }, age: { type: 'integer'} })};

      it('should use the AVG aggregate option in the select statement', function() {
        var query = new Sequel(schema, Support.SqlOptions).find('test', criteria);
        var sql = 'SELECT CAST( AVG("test"."age") AS float) AS age FROM "test" AS "test"  WHERE ' +
                  'LOWER("test"."name") = $1 ';

        query.query[0].should.eql(sql);
      });
    });

  });
});