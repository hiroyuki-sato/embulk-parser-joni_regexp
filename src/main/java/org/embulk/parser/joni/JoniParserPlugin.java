package org.embulk.parser.joni;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.util.LineDecoder;
import org.embulk.spi.util.Timestamps;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Locale;

public class JoniParserPlugin
        implements ParserPlugin
{
    private static final Logger logger = Exec.getLogger(JoniParserPlugin.class);

    public interface TypecastColumnOption
            extends Task
    {
        @Config("typecast")
        @ConfigDefault("null")
        public Optional<Boolean> getTypecast();
    }

    public interface PluginTask
            extends Task, LineDecoder.DecoderTask, TimestampParser.Task
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
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();

        validateSchema(task, schema);

        control.run(task.dump(), schema);
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        LineDecoder lineDecoder = new LineDecoder(input, task);
        PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output);
        TimestampParser[] timestampParsers = Timestamps.newTimestampColumnParsers(task, task.getColumns());

        ColumnVisitorImpl visitor = new ColumnVisitorImpl(task, schema, pageBuilder, timestampParsers);

        Regex regex = buildRegex(task);

        while (input.nextFile()) {
            while (true) {
                String line = lineDecoder.poll();
//                logger.debug(String.format(Locale.ENGLISH,"line = %s",line));
                if (line == null) {
                    break;
                }
                Matcher matcher = regex.matcher(line.getBytes());
                int result = matcher.search(0, line.getBytes().length, Option.DEFAULT);

                if (result != -1) {
                    Region region = matcher.getEagerRegion();
                    for (Iterator<NameEntry> entry = regex.namedBackrefIterator(); entry.hasNext(); ) {
                        NameEntry e = entry.next();
                        String name = captureName(e);

                        int number = e.getBackRefs()[0];
                        int begin = region.beg[number];
                        int end = region.end[number];
                        String strValue = new String(line.getBytes(StandardCharsets.UTF_8), begin, end - begin, StandardCharsets.UTF_8);

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
        byte[] pattern = format.getBytes();
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
}
