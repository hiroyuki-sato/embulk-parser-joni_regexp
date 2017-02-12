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
import org.joni.*;
import org.slf4j.Logger;

import org.jcodings.specific.UTF8Encoding;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Locale;


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

        @Config("format")
        String getFormat();

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

        String format = task.getFormat();
        logger.info(String.format(Locale.ENGLISH,"format = %s",format));
        byte[] pattern = format.getBytes();

        Regex regex = new Regex(pattern,0,pattern.length,Option.NONE,UTF8Encoding.INSTANCE);

        while (input.nextFile()) {
            while (true) {
                String line = lineDecoder.poll();
//                logger.debug(String.format(Locale.ENGLISH,"line = %s",line));
                if (line == null) {
                    break;
                }
                Matcher matcher = regex.matcher(line.getBytes());
                int result = matcher.search(0,line.getBytes().length,Option.DEFAULT);

                if( result != -1 ){
                     Region region = matcher.getEagerRegion();
                     for(Iterator<NameEntry> entry = regex.namedBackrefIterator(); entry.hasNext(); ){
                         NameEntry e = entry.next();
                         int number = e.getBackRefs()[0];
                         int begin = region.beg[number];
                         int end = region.end[number];

                         String str = new String(line.getBytes(StandardCharsets.UTF_8), begin, end - begin, StandardCharsets.UTF_8);
                         String name = new String(e.name, e.nameP, e.nameEnd - e.nameP);
                         logger.debug(String.format(Locale.ENGLISH,"<%s> = %s",name,str));
                     }
                } else {
                    logger.warn(String.format(Locale.ENGLISH,"unmatched line = %s",line));
                }
            }
        }

    }
}
