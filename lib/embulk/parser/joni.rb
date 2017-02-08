Embulk::JavaPlugin.register_parser(
  "joni", "org.embulk.parser.joni.JoniParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
