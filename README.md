# Sails-MySQL Adapter <a target="_blank" href="http://www.mysql.com"><img src="http://www.mysql.com/common/logos/powered-by-mysql-125x64.png" alt="Powered by MySQL" title="sails-mysql: MySQL adapter for Sails"/></a>

MySQL adapter for the Sails framework and Waterline ORM.  Allows you to use MySQL via your models to store and retrieve data.  Also provides a `query()` method for a direct interface to execute raw SQL commands.



## Installation

Install from NPM.

```bash
# In your app:
$ npm install sails-mysql
```

## Help

If you have further questions or are having trouble, click [here](http://sailsjs.com/support).


## Bugs &nbsp; [![NPM version](https://badge.fury.io/js/sails-mysql.svg)](http://npmjs.com/package/sails-mysql)

To report a bug, [click here](http://sailsjs.com/bugs).


## Contributing

Please observe the guidelines and conventions laid out in the [Sails project contribution guide](http://sailsjs.com/documentation/contributing) when opening issues or submitting pull requests.

[![NPM](https://nodei.co/npm/sails-mysql.png?downloads=true)](http://npmjs.com/package/sails-mysql)


#### Running the tests

To run the tests, point this adapter at your database by specifying a [connection URL](http://sailsjs.com/documentation/reference/configuration/sails-config-datastores#?the-connection-url) and run `npm test`:

```
WATERLINE_ADAPTER_TESTS_URL=mysql://root:myc00lP4ssw0rD@localhost/adapter_tests npm test
```

> For more info, see [**Reference > Configuration > sails.config.datastores > The connection URL**](http://sailsjs.com/documentation/reference/configuration/sails-config-datastores#?the-connection-url), or [ask for help](http://sailsjs.com/support).

## License

This adapter, like the [Sails framework](http://sailsjs.com) is free and open-source under the [MIT License](http://sailsjs.com/license).

