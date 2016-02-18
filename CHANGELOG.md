# Sails MySQL Changelog

### 0.11.5

* [BUG] Updates [Waterline-Sequel](https://github.com/balderdashy/waterline-sequel) dependency to actually fix the previous dates bug.

* [ENHANCEMENT] Changes the database url parsing to strip out query string values. See [#280](https://github.com/balderdashy/sails-mysql/pull/280) for more details. Thanks [@Bazze](https://github.com/Bazze)!

### 0.11.4

* [BUG] Updates [Waterline-Sequel](https://github.com/balderdashy/waterline-sequel) dependency to gain support for querying dates when they are represented as a string in the criteria.

* [ENHANCEMENT] Normalize the adapter errors some to be more in line with the Postgres driver. Now returns the `originalError` key as specified in [Waterline-Adapter-Tests](https://github.com/balderdashy/waterline-adapter-tests/pull/89).

### 0.11.3

* [BUG] Fixes issue with an outdated `.stream()` interface. See [#264](https://github.com/balderdashy/sails-mysql/pull/264) for more details. Thanks [@github1337](https://github.com/github1337) for the patch!

* [ENHANCEMENT] Better error message in the case of a foreign key constraint violation. See [#268](https://github.com/balderdashy/sails-mysql/pull/268) for more details. Thanks [@trheming](https://github.com/trheming) for the patch!

* [ENHANCEMENT] Locked the dependency versions down to know working versions. Also added a `shrinkwrap.json` file. See [#272](https://github.com/balderdashy/sails-mysql/pull/272) for more details.

* [ENHANCEMENT] Updated the Travis config to run test on Node 4.0 and 5.0. See [#273](https://github.com/balderdashy/sails-mysql/pull/273) for more details.

* [PERFORMANCE] And the best for last, merged [#274](https://github.com/balderdashy/sails-mysql/pull/274) which increases performance on populates ~15x. Thanks a million to [@jianpingw](https://github.com/jianpingw) for spending the time to track this down!
