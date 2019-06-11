require 'redis'
require 'json'
require 'active_support/all'
require 'shellwords'

redis = Redis.new

loop do
  json_object = redis.lpop 'gnip_powertrack'
  if json_object.blank?
    sleep(1)
  else
    begin
      json_hash = JSON.parse json_object
      unless json_hash['objectType'] == 'activity'
        File.open('twitter_unprocessed_data.json', 'a') do |file|
          file.puts json_object
        end
      end
    rescue => e
      File.open('twitter_processing_error.log', 'a') do |f|
        f.puts "---------------------------------------"
        f.puts json_object
        f.puts "------------------------------------------------"
        f.puts e.message
        f.puts "============================================="
      end
      puts "error"
    end
  end
end

twitter_data.close
twitter_unprocessed_data.close
