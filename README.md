mssql2postgres
==============

Ruby script for migrating a MSSQL database to Postgres. Source database credentials should be added to a file called `db_creds` in the following format:

    <database username>
    <database password>
    <hostname>
    <database name>

Destination database credentials should be added in the same format to a file called `dest_db_creds`. Note the database will have to be created on the server first.

Tables that you want to ignore can be added to blacklist.yml in a YAML array format.

You can then run the script by running `bundle install` and then `bundle exec ruby migrate.rb`
