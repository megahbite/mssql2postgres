require 'tiny_tds'
require 'yaml'
require 'pg'

def map_types(type_name, length, scale, precision, is_identity)
  case type_name
  #Numeric types
  when "bigint"
    return is_identity ? "bigserial" : "bigint"
  when "bit"
    return "boolean"
  when "decimal", "numeric"
    return "numeric(#{precision}, #{scale})"
  when "int"
    return is_identity ? "serial" : "integer"
  when "money", "smallmoney"
    return "numeric(12, 2)"
  when "smallint"
    return is_identity ? "smallserial" : "smallint"
  when "tinyint"
    return "smallint"
  when "float"
    if precision == 15
      return "double precision"
    else
      return "real"
    end
  when "real"
    return "real"

  #Dates/Times
  when "date"
    return "date"
  when "datetime"
    return "timestamp" 
  when "datetime2"
    return "timestamp(#{precision})"
  when "datetimeoffset"
    return "timestamp(#{precision}) with time zone"
  when "time"
    return "time(#{precision})"

  #Character types
  when "char", "nchar"
    return "character(#{length})"
  when "varchar", "nvarchar"
    if length == -1
      return "character varying"
    else
      return "character varying(#{length})"
    end
  when "text", "ntext"
    return "text"
  when "binary", "varbinary", "image"
    return "bytea"

  #Other
  when "uniqueidentifier"
    return "uuid"
  when "xml"
    return "xml"
  else
    return "character varying"
  end
end

credentials_file = File.new('db_creds', 'r')

if not credentials_file
  raise "No credentials file"
end

username = credentials_file.readline.strip
password = credentials_file.readline.strip
host = credentials_file.readline.strip
database = credentials_file.readline.strip

credentials_file.close

db_client = TinyTds::Client.new(username: username, password: password, host: host, database: database)

tables = []

puts ">> Fetching tables"
#Gather info on tables
db_client.execute('SELECT * FROM sys.tables ORDER BY name').each do |row|
  tables << { name: row["name"], object_id: row["object_id"] }
end

puts "#{tables.length} tables fetched"


#Get column info
tables.each do |table|
  puts ">> Fetching columns for #{table[:name]}"
  columns = []
  db_client.execute("SELECT column_id as id, name, TYPE_NAME(user_type_id) as type, max_length, is_identity, is_nullable, scale, precision FROM sys.columns WHERE object_id = #{table[:object_id]} ORDER BY column_id").each do |row|
    columns << { 
      id: row["id"],
      name: row["name"], 
      type: row["type"], 
      max_length: row["max_length"], 
      is_identity: row["is_identity"], 
      is_nullable: row["is_nullable"], 
      scale: row["scale"], 
      precision: row["precision"],
      is_primary_key: false
    }
  end
  table.merge!({columns: columns})
  puts "#{columns.length} columns fetched"
end

#Get primary key info
tables.each do |table|
  puts ">> Fetching primary keys for #{table[:name]}"
  db_client.execute("SELECT ic.column_id FROM 
    sys.indexes AS i 
    INNER JOIN sys.index_columns AS ic ON 
    i.object_id = ic.object_id AND i.index_id = ic.index_id 
    WHERE i.is_primary_key = 1 AND i.object_id = #{table[:object_id]}").each do |row|
    column = table[:columns].select { |c| c[:id] == row["column_id"] }.first
    column.merge!({ is_primary_key: true })
    puts "Primary key set on column #{column[:name]}"
  end
end

dest_credentials_file = File.new('dest_db_creds', 'r')

if not dest_credentials_file
  raise "No destination credentials file"
end

username = dest_credentials_file.readline.strip
password = dest_credentials_file.readline.strip
host = dest_credentials_file.readline.strip
database = dest_credentials_file.readline.strip

dest_credentials_file.close

dest_db_client = PG::connect(host: host, dbname: database, user: username, password: password)

#Create tables on postgres
tables.each do |table|
  dest_db_client.exec("DROP TABLE IF EXISTS #{table[:name]}")

  create_sql = "CREATE TABLE #{table[:name]} ("

  table[:columns].each do |column|
    create_sql += "#{column[:name]} #{map_types(column[:type], column[:max_length], column[:scale], 
      column[:precision], column[:is_identity])} "
    
    create_sql += "NOT NULL " unless column[:is_nullable]

    create_sql += ", "
  end

  keys = table[:columns].select { |c| c[:is_primary_key] }.map { |c| c[:name] }

  create_sql += "CONSTRAINT #{table[:name]}_#{keys.join('_')}_key PRIMARY KEY(#{keys.join(',')})" if keys.length > 0

  create_sql.chomp!(" , ")
  create_sql += ")"

  dest_db_client.exec(create_sql)
end
