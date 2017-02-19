Embulk::JavaPlugin.register_parser(
  "joni_regexp", "org.embulk.parser.joni_regexp.JoniRegexpParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
