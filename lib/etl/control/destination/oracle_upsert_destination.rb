module ETL #:nodoc:
  module Control #:nodoc:
    # Destination which writes directly to a database. This is useful when you are dealing with
    # a small amount of data. For larger amounts of data you should probably use the bulk
    # loader if it is supported with your target database as it will use a much faster load
    # method.
    class OracleUpsertDestination < DatabaseDestination
      def initialize(control, configuration, mapping={})
        super
        @insert_ts_column = configuration[:insert_ts_column]
        @update_ts_column = configuration[:update_ts_column]
        @key_columns = configuration[:key_columns]
        raise ControlError,"Must provide attributes columns" unless @insert_ts_column and @update_ts_column and @key_columns
      end
      
      # Flush the currently buffered data
      def flush
        conn.transaction do
          buffer.flatten.each do |row|
            # check to see if this row's compound key constraint already exists
            # note that the compound key constraint may not utilize virtual fields
            next unless row_allowed?(row)

            # add any virtual fields
            add_virtuals!(row)
            
            key_names = []
            key_values = []
            @key_columns.each do |name|
              key_names << "#{name}"
              key_values << conn.quote(row[name]) # TODO: this is probably not database agnostic
            end

            names = []
            values = []
            (order - @key_columns).each do |name|
              names << "#{name}"
              values << conn.quote(row[name]) # TODO: this is probably not database agnostic
            end

            all_name_values = (key_names+names).zip(key_values+values)

            q = <<EOF
MERGE INTO #{table_name} d 
USING (SELECT #{all_name_values.collect {|c,v| "#{v} #{c}"}.join(',')} FROM DUAL) s
ON (#{map_src_to_dest(key_names,'s','d').join(' AND ')})
WHEN MATCHED THEN 
UPDATE SET #{[map_src_to_dest(names,'s','d'), "d.#{@update_ts_column}=CURRENT_TIMESTAMP"].flatten.join(',')}
WHEN NOT MATCHED THEN
INSERT (#{all_name_values.collect {|c,v| 'd.'+c}.join(',')},d.#{@insert_ts_column})
VALUES (#{all_name_values.collect {|c,v| 's.'+c}.join(',')},CURRENT_TIMESTAMP)
EOF
            #q = "INSERT INTO `#{table_name}` (#{names.join(',')}) VALUES (#{values.join(',')})"
            ETL::Engine.logger.debug("Executing upsert: #{q}")
            conn.insert(q, "Upsert row #{current_row}")
            @current_row += 1
          end
          buffer.clear
        end
      end

protected
      def map_src_to_dest(columns,source_alias,dest_alias)
        columns.collect{|col| dest_alias + '.' + col + '=' + source_alias + '.' + col}
      end
    end
  end
end
