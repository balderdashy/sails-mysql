# Sails-MySQL Adapter <a target="_blank" href="http://www.mysql.com"><img src="http://www.mysql.com/common/logos/powered-by-mysql-125x64.png" alt="Powered by MySQL" title="sails-mysql: MySQL adapter for Sails"/></a>

MySQL adapter for the Sails framework and Waterline ORM.  Allows you to use MySQL via your models to store and retrieve data.  Also provides a `query()` method for a direct interface to execute raw SQL commands.



## Installation

Install from NPM.

```bash
# In your app:
$ npm install sails-mysql
```

## Sails Configuration

Add the mysql config to the config/connections.js file. Basic options:

```javascript
module.exports.connections = {
  mysql: {
    adapter   : 'sails-mysql',
    host      : 'localhost',
    port      : 3306,
    user      : 'username',
    password  : 'password',
    database  : 'MySQL Database Name'

    // OR (explicit sets take precedence)
    adapter   : 'sails-mysql',
    url       : 'mysql2://USER:PASSWORD@HOST:PORT/DATABASENAME'

    // Optional
    charset   : 'utf8',
    collation : 'utf8_swedish_ci'
  }
};
```

And then change default model configuration to the config/models.js:

```javascript
module.exports.models = {
  connection: 'mysql'
};
```

## Run tests

You can set environment variables to override the default database config for the tests, e.g.:

```sh
$ WATERLINE_ADAPTER_TESTS_PASSWORD=yourpass npm test
```


Default settings are:

```javascript
{
  host: process.env.WATERLINE_ADAPTER_TESTS_HOST || 'localhost',
  port: process.env.WATERLINE_ADAPTER_TESTS_PORT || 3306,
  user: process.env.WATERLINE_ADAPTER_TESTS_USER || 'root',
  password: process.env.WATERLINE_ADAPTER_TESTS_PASSWORD || '',
  database: process.env.WATERLINE_ADAPTER_TESTS_DATABASE || 'sails_mysql',
  pool: true,
  connectionLimit: 10,
  waitForConnections: true
}
```

## Help

If you have further questions or are having trouble, click [here](http://sailsjs.com/support).


## Bugs &nbsp; [![NPM version](https://badge.fury.io/js/sails-mysql.svg)](http://npmjs.com/package/sails-mysql)

To report a bug, [click here](http://sailsjs.com/bugs).


## Contributing

Please observe the guidelines and conventions laid out in the [Sails project contribution guide](http://sailsjs.com/contribute) when opening issues or submitting pull requests.

[![NPM](https://nodei.co/npm/sails-mysql.png?downloads=true)](http://npmjs.com/package/sails-mysql)


## License

The [Sails framework](http://sailsjs.com) is free and open-source under the [MIT License](http://sailsjs.com/license).

