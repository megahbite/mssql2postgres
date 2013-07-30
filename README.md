mssql2postgres
==============

Ruby script for migrating a MSSQL database to Postgresql. 

Source database credentials should be added to a file called `db_creds.yml` in a YAML hash format. Available options can be [found here](https://github.com/rails-sqlserver/tiny_tds/#tinytdsclient-usage)

Destination database credentials should be added also in a hash format to a file called `dest_db_creds.yml`. Note the database will have to be created on the server first. Options can be [found here](http://deveiate.org/code/pg/PG/Connection.html#method-c-new)

Tables that you want to ignore can be added to `blacklist.yml` in a YAML array format.

You can then run the script by running `bundle install` and then `bundle exec ruby migrate.rb`
