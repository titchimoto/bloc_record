require 'sqlite3'
require 'pg'

module Selection

    def find(*ids)
      # filter *ids to include only integers larger than 0.
      ids.select  { |item| item.is_a? Integer and item > 0 }

        if ids.length == 1
            find_one(ids.first)
        else
            rows = connection.execute <<-SQL
              SELECT #{columns.join ","} FROM #{table}
              WHERE id IN (#{ids.join(",")});
            SQL

            rows_to_array(rows)
        end
    end

    def find_one(id)
      if id.is_a? Integer and id > 0
        row = connection.get_first_row <<-SQL
            SELECT #{columns.join ","} FROM #{table}
            WHERE id = #{id}
        SQL

        init_object_from_row(row)
      else
        raise ArgumentError, "That is not a valid ID. ID's must be positive whole numbers."
      end
    end

    def find_by(attribute, value)
      if columns.include?(attribute)
        rows = connection.execute <<-SQL
          SELECT #{columns.join ","} FROM #{table}
          WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
        SQL

        rows_to_array(rows)
      else
        raise ArgumentError, "#{attribute} is not a valid attribute. Please try again."
      end
    end

    def take_one
        row = connection.execute <<-SQL
          SELECT #{columns.join ","} FROM #{table}
          ORDER BY random()
          LIMIT 1;
        SQL

        init_object_from_row(row)
    end

    def take(num=1)
      if num.is_a? Integer
        if num > 1
            rows = connection.execute <<-SQL
              SELECT #{columns.join ","} FROM #{table}
              ORDER BY random()
              LIMIT #{num};
            SQL

            rows_to_array(rows)
        else
            take_one
        end
      else
        raise ArgumentError, "Please use a valid whole number."
      end
    end

    def first
        row = connection.get_first_row <<-SQL
          SELECT #{columns.join ","} FROM #{table}
          ORDER BY id ASC
          LIMIT 1;
        SQL

        init_object_from_row(row)
    end

    def last
        row = connection.execute <<-SQL
          SELECT #{columns.join ","} FROM #{table}
          ORDER BY id DESC
          LIMIT 1;
        SQL

        init_object_from_row(row)
    end

    def all
        rows = connection.execute <<-SQL
          SELECT #{columns.join ","} FROM #{table}; 
        SQL

        rows_to_array(rows)
    end

    def method_missing(method, *args, &block)
        if method.to_s =~ /find_by_(.*)/
            find_by($1, *args[0])
        else
            super
        end
    end

    def find_each(options = {})

        start = options[:start] ||= 0
        batch_size = options[:batch_size] ||= 1000
        
        rows = connection.execute <<-SQL
          SELECT #{columns.join ","} FROM #{table}
          ORDER BY id
          LIMIT #{batch_size} OFFSET #{start};
        SQL
        
        all_rows = rows_to_array(rows)
        all_rows.each do |row|
          yield(row)
        end
    end

    def find_in_batches(options = {})

        start = options[:start] ||= 0
        batch_size = options[:batch_size] ||= 1000
        
        rows = connection.execute <<-SQL
          SELECT #{columns.join ","} FROM #{table}
          ORDER BY id
          LIMIT #{batch_size} OFFSET #{start};
        SQL
        
        rows_to_array(rows)
    end

    def where(*args)
      if args.count > 1
        expression = args.shift
        params = args
      else
        case args.first
        when String
          expression = args.first
        when Hash
          expression_hash = BlocRecord::Utility.convert_keys(args.first)
          expression = expression_hash.map { |key, value|  "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
        end
      end

      sql = <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE #{expression};
      SQL

      rows = connection.execute(sql, params)
      rows_to_array(rows)
    end

    def order(*args)
      args_array = []
      args.each do |arg|
        if arg.class == String || arg.class == Symbol
          args_array << arg.to_s
        elsif arg.class == Hash
          args_array << arg.map {|key, value| "#{key} #{value}"}
        end
      end

      order = args_array.join(", ")

      rows = connection.execute <<-SQL
        SELECT * FROM #{table}
        ORDER BY #{order};
      SQL
      rows_to_array(rows)
    end

    def join(*args)
      if args.count > 1
        joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}.#{table}_id = #{table}.id"}.join(" ")
        rows = connection.execute <<-SQL
          SELECT * FROM #{table} #{joins};
        SQL
      else
        case args.first
        when String
          rows = connection.execute <<-SQL
            SELECT * FROM #{table} #{BlocRecord::Utility.sql_strings(args.first)};
          SQL
        when Symbol
          rows = connection.execute <<-SQL
            SELECT * FROM #{table} 
            INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id;
          SQL
        when Hash
          table2 = args[0].keys[0]
          table3 = args[0][table2]
          rows = connection.execute <<-SQL
            SELECT * FROM #{table}
            INNER JOIN #{table2} ON #{table2}.#{table}_id = #{table}.id
            INNER JOIN #{table3} ON #{table3}.#{table2}_id = #{table2}.id;
          SQL
        end
      end
      rows_to_array(rows)
    end

    private

    def init_object_from_row(row)
        if row
            data = Hash[columns.zip(row)]
            new(data)
        end
    end
    
    def rows_to_array(rows)
        collection = BlocRecord::Collection.new
        rows.each { |row| collection << new(Hash[columns.zip(row)]) }
        collection
    end
end



