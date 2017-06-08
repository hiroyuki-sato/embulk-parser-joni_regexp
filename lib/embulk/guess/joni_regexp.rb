module Embulk
  module Guess

    class JoniRegexp < GuessPlugin
      Plugin.register_guess("joni_regexp", self)
    
      def guess(config, sample_buffer)
        parser_config = config["parser"]
        return {} unless parser_config
        format = parser_config["format"]
        raise StandardError,"Guess need string `format` parameter" unless format.kind_of?(String)
#        raise ConfigError,"Guess need string `format` parameter" unless format.kind_of?(String)
        guessed = {}
        begin
          regex = Regexp.new(format)
          columns = []
          guessed["type"] = "joni_regexp"
          guessed["format"] = format
          columns = regex.names.map{ |x| {'name' => x, 'type' => 'string'} }
          guessed["columns"] = columns
          return {"parser" => guessed}
        rescue
          return {}
        end
      end
    end
  end
end
