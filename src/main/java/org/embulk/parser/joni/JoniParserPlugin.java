package org.embulk.parser.joni;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.PageBuilder;

import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.Exec;
import org.embulk.spi.util.Timestamps;

import org.embulk.spi.util.LineDecoder;
import org.slf4j.Logger;

import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Regex;


public class JoniParserPlugin
        implements ParserPlugin
{
    private static final Logger logger = Exec.getLogger(JoniParserPlugin.class);

    public interface PluginTask
            extends Task,LineDecoder.DecoderTask, TimestampParser.Task
    {
        @Config("columns")
        SchemaConfig getColumns();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        boolean getStopOnInvalidRecord();

    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();

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

        while (input.nextFile()) {
            while (true) {
                String line = lineDecoder.poll();

                if (line == null) {
                    break;
                }

            }
        }

    }
}
