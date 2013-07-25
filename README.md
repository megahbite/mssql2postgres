mssql2postgres
==============

Ruby script for migrating a MSSQL database to Postgres. Source database credentials should be added to a file called `db_creds` in the following format:

    <database username>
    <database password>
    <hostname>
    <database name>

You can then run the script by running `bundle install` and then `bundle exec ruby migrate.rb`
