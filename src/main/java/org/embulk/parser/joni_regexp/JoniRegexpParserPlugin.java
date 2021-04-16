package org.embulk.parser.joni_regexp;

import java.util.Optional;

import org.embulk.spi.type.TimestampType;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.TypeModule;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.text.LineDecoder;
import org.embulk.util.text.LineDelimiter;
import org.embulk.util.text.Newline;
import org.embulk.util.timestamp.TimestampFormatter;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Locale;

public class JoniRegexpParserPlugin
        implements ParserPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(JoniRegexpParserPlugin.class);
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
            .builder()
            .addDefaultModules()
            .addModule(new TypeModule())
            .build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    public interface TypecastColumnOption
            extends Task
    {
        @Config("typecast")
        @ConfigDefault("null")
        public Optional<Boolean> getTypecast();
    }

    public interface PluginTask
            extends Task
    {
        @Config("columns")
        SchemaConfig getColumns();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        boolean getStopOnInvalidRecord();

        @Config("format")
        String getFormat();

        @Config("default_typecast")
        @ConfigDefault("true")
        Boolean getDefaultTypecast();

        // From org.embulk.spi.util.LineDecoder.DecoderTask.
        @Config("charset")
        @ConfigDefault("\"utf-8\"")
        Charset getCharset();

        // From org.embulk.spi.util.LineDecoder.DecoderTask.
        @Config("newline")
        @ConfigDefault("\"CRLF\"")
        Newline getNewline();

        // From org.embulk.spi.util.LineDecoder.DecoderTask.
        @Config("line_delimiter_recognized")
        @ConfigDefault("null")
        Optional<LineDelimiter> getLineDelimiterRecognized();

        // From org.embulk.spi.time.TimestampParser.Task.
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        String getDefaultTimeZoneId();

        // From org.embulk.spi.time.TimestampParser.Task.
        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        String getDefaultTimestampFormat();

        // From org.embulk.spi.time.TimestampParser.Task.
        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        String getDefaultDate();
    }

    public interface JoniRegexpColumnOption
            extends Task
    {
        @Config("timezone")
        @ConfigDefault("null")
        Optional<String> getTimeZoneId();

        @Config("format")
        @ConfigDefault("null")
        Optional<String> getFormat();

        @Config("date")
        @ConfigDefault("null")
        Optional<String> getDate();
    }
    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        Schema schema = task.getColumns().toSchema();

        validateSchema(task, schema);

        control.run(task.dump(), schema);
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
        final LineDecoder lineDecoder = LineDecoder.of(input, task.getCharset(), task.getLineDelimiterRecognized().orElse(null));
        PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output);
        TimestampFormatter[] timestampParsers = newTimestampColumnFormatters(task, task.getColumns());

        ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, schema, pageBuilder, timestampParsers);

        Regex regex = buildRegex(task);

        while (input.nextFile()) {
            while (true) {
                String line = lineDecoder.poll();
//                logger.debug(String.format(Locale.ENGLISH,"line = %s",line));
                if (line == null) {
                    break;
                }
                byte[] line_bytes = line.getBytes(StandardCharsets.UTF_8);
                Matcher matcher = regex.matcher(line_bytes);
                int result = matcher.search(0, line_bytes.length, Option.DEFAULT);

                if (result != -1) {
                    Region region = matcher.getEagerRegion();
                    for (Iterator<NameEntry> entry = regex.namedBackrefIterator(); entry.hasNext(); ) {
                        NameEntry e = entry.next();
                        String name = captureName(e);

                        int number = e.getBackRefs()[0];
                        int begin = region.beg[number];
                        int end = region.end[number];
                        String strValue = new String(line_bytes, begin, end - begin, StandardCharsets.UTF_8);

                        logger.debug(String.format(Locale.ENGLISH, "<%s> = %s", name, strValue));
                        setValue(schema, visitor, name, strValue);
                    }
                    pageBuilder.addRecord();
                }
                else if (task.getStopOnInvalidRecord() == false) {
                    logger.warn(String.format(Locale.ENGLISH, "skip unmatched line = %s", line));
                }
                else {
                    throw new DataException(String.format("Invalid record at line %s", line));
                }
            }
        }
        pageBuilder.finish();
    }

    private void setValue(Schema schema, ColumnVisitorImpl visitor, String name, String strValue)
    {
        try {
            Value value = ValueFactory.newString(strValue);
            Column column = schema.lookupColumn(name);
            visitor.setValue(value);
            column.visit(visitor);
        }
        catch (Exception ex) {
            throw new DataException(String.format(Locale.ENGLISH, "Set value failed. column = \"%s\" value = \"%s\", reason = \"%s\"", name, strValue, ex.getMessage()));
        }
    }

    private Regex buildRegex(PluginTask task)
    {
        String format = task.getFormat();
        byte[] pattern = format.getBytes(StandardCharsets.UTF_8);
        // throw org.joni.exception.SyntaxException if regex is invalid.
        return new Regex(pattern, 0, pattern.length, Option.NONE, UTF8Encoding.INSTANCE);
    }

    private String captureName(NameEntry e)
    {
        return new String(e.name, e.nameP, e.nameEnd - e.nameP);
    }

    private void validateSchema(PluginTask task, Schema schema)
    {
        Regex regex = buildRegex(task);
        if (regex.numberOfNames() < 1) {
            throw new ConfigException("The regex has no named capturing group");
        }
        for (Iterator<NameEntry> entry = regex.namedBackrefIterator(); entry.hasNext(); ) {
            NameEntry e = entry.next();
            String captureName = captureName(e);
            schema.lookupColumn(captureName); // throw SchemaConfigException;
        }
    }
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1289
    private static TimestampFormatter[] newTimestampColumnFormatters(
            final PluginTask task,
            final SchemaConfig schema) {
        final TimestampFormatter[] formatters = new TimestampFormatter[schema.getColumnCount()];
        int i = 0;
        for (final ColumnConfig column : schema.getColumns()) {
            if (column.getType() instanceof TimestampType) {
                final JoniRegexpColumnOption columnOption =
                        CONFIG_MAPPER.map(column.getOption(), JoniRegexpColumnOption.class);

                final String pattern = columnOption.getFormat().orElse(task.getDefaultTimestampFormat());
                formatters[i] = TimestampFormatter.builder(pattern, true)
                        .setDefaultZoneFromString(columnOption.getTimeZoneId().orElse(task.getDefaultTimeZoneId()))
                        .setDefaultDateFromString(columnOption.getDate().orElse(task.getDefaultDate()))
                        .build();
            }
            i++;
        }
        return formatters;
    }
}
