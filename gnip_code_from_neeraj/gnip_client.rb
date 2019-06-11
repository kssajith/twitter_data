require 'net/http'
require 'json'
require 'redis'
require 'colorize'
require 'active_support/all'

class GNIPFatalException < StandardError; end

$redis = Redis.new
uri = URI('https://gnip-stream.twitter.com/stream/powertrack/accounts/mobme/publishers/twitter/prod.json')

def queue_to_redis(objects)
  objects.each do |object|
    $redis.rpush('gnip_powertrack', object)
  end
end

retry_count = 1

begin
  Net::HTTP.start(uri.host, uri.port, :use_ssl => uri.scheme == 'https') do |http|
    puts "[#{Time.now.to_s(:db)}] INFO: calling get".green
    request = Net::HTTP::Get.new uri
    puts "[#{Time.now.to_s(:db)}] INFO: authenticating".green
    request.basic_auth 'neeraj@mobme.in', 'mobme@123'

    puts "[#{Time.now.to_s(:db)}] INFO: requesting".green
    stream = ""
    http.request request do |response|
      puts "[#{Time.now.to_s(:db)}] INFO: read body".green
      response.read_body do |chunk|
        stream += chunk
        objects = stream.split(/\r?\n/)

        if stream.length > 0 and objects.count == 1
          data = JSON.load(stream)
          if data.include? 'error'
            raise GNIPFatalException.new(data['error']['message'])
          end
        end

        if stream.length > 0 and retry_count > 0
          puts "Resetting retry_count to 0"
          retry_count = 0
        end
        
        if /\r?\n/ =~ chunk[-1]
          stream = ""
          queue_to_redis(objects)
        else
          stream = objects[-1] unless objects.empty?
          queue_to_redis(objects[0..-2])
        end
      end
    end
  end
rescue GNIPFatalException => e
  raise e
rescue Exception => e
  puts e.message
  error_filename = 'gnip_error.log'
  sleep_time = 2**retry_count
  puts "[#{Time.now.to_s(:db)}] ERROR: Exception occurred, sleeping for #{sleep_time} seconds before reconnecting".red
  puts "[#{Time.now.to_s(:db)}] DEBUG: For further details on error look in #{error_filename}".blue
  sleep(sleep_time)
  puts retry_count.inspect
  retry_count += 1

  File.open('gnip_error.log', 'a') do |file|
    file.puts "[#{Time.now.to_s(:db)}] ERROR:\n#{e.message}"
  end
  retry
end
