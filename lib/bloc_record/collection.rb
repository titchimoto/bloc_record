module BlocRecord
    class Collection < Array
        def update_all(updates)
            ids = self.map(&:id)
            self.any? ? self.first.class.update(ids, updates) : false
        end

        def take(num=1)
            take_array = []
            count = 0
            while count < num
                take_array << self[count]
                count += 1
            end
            take_array
        end

        def where(params)
            results = BlocRecord::Collection.new
            params_total = params.keys.length
      
            self.each do |item|
              params_count = 0
              params.each do |key, val|
                if item.send(key) == val
                  params_count += 1
                    if params_total == params_count && !results.include?(item)
                        results << item
                    end
                end
              end
            end
            results
        end
      
        def not(params)
            results = BlocRecord::Collection.new
            self.each do |item|
                params.each do |key, val|
                    if item.send(key) != val && !results.include?(item)
                        results << item
                    end
                end
            end
            results
        end

        def destroy
            self.each do |item|
                item.destroy
                puts "#{item} was successfully deleted from the database"
            end
        end
    end
end
