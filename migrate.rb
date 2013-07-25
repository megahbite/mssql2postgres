require 'tiny_tds'
require 'yaml'

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

yaml_file = File.new('table_schema.yaml', 'w')
yaml_file.write(tables.to_yaml())
yaml_file.close