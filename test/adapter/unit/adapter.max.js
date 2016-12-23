var Sequel = require('waterline-sequel'), 
    should = require('should'),
    Support = require('./support/bootstrap');

describe('query', function() {

  /**
   * MAX
   *
   * Adds a MAX select parameter to a sql statement
   */

  describe('.max()', function() {

    describe('with array', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        max: ['age']
      };

      var schema = {'test': Support.Schema('test', { name: { type: 'text' }, age: { type: 'integer'} })};

      it('should use the max aggregate option in the select statement', function() {
        var query = new Sequel(schema, Support.SqlOptions).find('test', criteria);
        var sql = 'SELECT MAX("test"."age") AS age FROM "test" AS "test"  WHERE LOWER("test"."name") = $1 ';
        query.query[0].should.eql(sql);
      });
    });

    describe('with string', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        max: 'age'
      };

      var schema = {'test': Support.Schema('test', { name: { type: 'text' }, age: { type: 'integer'} })};

      it('should use the MAX aggregate option in the select statement', function() {
        var query = new Sequel(schema, Support.SqlOptions).find('test', criteria);
        var sql = 'SELECT MAX("test"."age") AS age FROM "test" AS "test"  WHERE LOWER("test"."name") = $1 ';
        query.query[0].should.eql(sql);
      });
    });

  });
});