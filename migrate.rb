require 'tiny_tds'
require 'yaml'
require 'pg'
require 'csv'

PAGE_SIZE = 5000

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
    raise "Unrecognised or unsupported MSSQL type #{type_name}"
  end
end

def next_page(db, table_name, field_list, order_column, page, page_size)
  select_sql = "WITH foo AS (SELECT #{field_list}, ROW_NUMBER() OVER(ORDER BY #{order_column}) as row_num FROM #{table_name}) 
  SELECT #{field_list} FROM foo WHERE row_num BETWEEN #{(page - 1) * page_size - 1} AND #{page * page_size}" 

  db.execute(select_sql)
end

def copy_data_to(db, table, columns, from)
  db.exec("COPY #{table} (#{columns.map { |c| c[:name] }.join(", ")}) FROM STDIN WITH CSV")
  begin
    from.each(cache_rows: false) do |row|
      row.keys.each do |k|
        unless columns.select { |c| c[:name] == k and ["binary", "varbinary", "image"].include?(c[:type]) }.empty?
          row[k] = db.escape_bytea(row[k]) unless row[k] == nil
        end

        unless columns.select { |c| c[:name] == k and ["char", "nchar", "varchar", "nvarchar", "text", "ntext"].include?(c[:type]) }.empty?
          row[k] = db.escape_string(row[k]) unless row[k] == nil
        end
      end
      buf = row.values.to_csv

      until db.put_copy_data(buf)
        sleep 0.1
      end
    end
  rescue Errno => err
    errmsg = "%s while reading copy data: %s" % [ err.class.name, err.message ]
    db.put_copy_end( errmsg )
  else
    db.put_copy_end
    while res = db.get_result
      puts "Result of COPY is: %s" % [ res.res_status(res.result_status) ]
    end
  end
end

def get_column_metadata(db, object_id)
  columns = []
  db.execute("SELECT column_id as id, name, TYPE_NAME(user_type_id) as type, max_length, is_identity, is_nullable, scale, precision 
    FROM sys.columns WHERE object_id = #{object_id} ORDER BY column_id").each do |row|
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
  columns
end

def read_credentials(file_handle)
  {
    username: file_handle.readline.strip,
    password: file_handle.readline.strip,
    host: file_handle.readline.strip,
    database: file_handle.readline.strip
  }
end

def symbolize_keys(hash)
  hash.keys.each do |key|
    hash[(key.to_sym rescue key) || key] = hash.delete(key)
  end
end

# tables to ignore
blacklist = YAML.load_file('blacklist.yml')


creds = YAML.load_file('db_creds.yml')
symbolize_keys(creds)
creds.merge!({timeout: 0})

mssql_conn = TinyTds::Client.new(creds)

tables = []

puts ">> Fetching tables"
#Gather info on tables
mssql_conn.execute('SELECT * FROM sys.tables ORDER BY name').each do |row|
  tables << { name: row["name"], object_id: row["object_id"] } unless blacklist.include?(row["name"])
end

puts "#{tables.length} tables fetched"


#Get column info
tables.each do |table|
  puts ">> Fetching columns for #{table[:name]}"
  table.merge!({columns: get_column_metadata(mssql_conn, table[:object_id])})
  puts "#{table[:columns].length} columns fetched"
end

#Get primary key info
tables.each do |table|
  puts ">> Fetching primary keys for #{table[:name]}"
  mssql_conn.execute("SELECT ic.column_id FROM 
    sys.indexes AS i 
    INNER JOIN sys.index_columns AS ic ON 
    i.object_id = ic.object_id AND i.index_id = ic.index_id 
    WHERE i.is_primary_key = 1 AND i.object_id = #{table[:object_id]}").each do |row|
    column = table[:columns].select { |c| c[:id] == row["column_id"] }.first
    column.merge!({ is_primary_key: true })
    puts "Primary key set on column #{column[:name]}"
  end
end

creds = YAML.load_file("dest_db_creds.yml")
symbolize_keys(creds)

pg_conn = PG::connect(creds)

#Create tables on postgres
tables.each do |table|
  pg_conn.exec("DROP TABLE IF EXISTS #{table[:name]}")

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

  pg_conn.exec(create_sql)
end

#Pull data and push to postgres
tables.each do |table|
  field_list = table[:columns].map { |c| "[" + c[:name] + "]" }.join(", ")
  
  page = 1

  result = next_page(mssql_conn, table[:name], field_list, table[:columns].first[:name], page, PAGE_SIZE)

  while result.count > 0
    pg_conn.transaction do
      puts "COPYing data to #{table[:name]}, page #{page}"
      copy_data_to(pg_conn, table[:name], table[:columns], result)
    end

    page += 1

    result = next_page(mssql_conn, table[:name], field_list, table[:columns].first[:name], page, PAGE_SIZE)
  end
end

mssql_conn.close
pg_conn.close
