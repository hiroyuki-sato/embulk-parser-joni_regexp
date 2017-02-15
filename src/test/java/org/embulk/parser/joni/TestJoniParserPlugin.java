package org.embulk.parser.joni;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.util.Newline;
import org.joda.time.DateTimeZone;
import org.junit.Rule;
import org.junit.Test;

import java.nio.charset.Charset;

import static junit.framework.TestCase.assertEquals;

public class TestJoniParserPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void checkDefaultValues() {
        ConfigSource config = Exec.newConfigSource()
                .set("columns", ImmutableList.of(
                        ImmutableMap.of(
                                "name", "name",
                                "type", "string")))
                .set("format", "(?<name>a*)");

        JoniParserPlugin.PluginTask task = config.loadConfig(JoniParserPlugin.PluginTask.class);

        assertEquals(Charset.forName("utf-8"), task.getCharset());
        assertEquals(Newline.CRLF, task.getNewline());
        assertEquals(DateTimeZone.UTC, task.getDefaultTimeZone());
        assertEquals("%Y-%m-%d %H:%M:%S.%N %z", task.getDefaultTimestampFormat());
        assertEquals( false, task.getStopOnInvalidRecord());
        //assertEquals( true, task.getDefaultTypecast());

    }

    @Test(expected = ConfigException.class)
    public void checkColumnsRequired()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("format", "(?<name>a*)");

        config.loadConfig(JoniParserPlugin.PluginTask.class);
    }

    @Test(expected = ConfigException.class)
    public void checkFormatRequired()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("columns", ImmutableList.of(
                        ImmutableMap.of(
                                "name", "name",
                                "type", "string")));

        config.loadConfig(JoniParserPlugin.PluginTask.class);
    }
}
