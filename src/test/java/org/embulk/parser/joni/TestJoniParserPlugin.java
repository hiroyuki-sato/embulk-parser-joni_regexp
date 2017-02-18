package org.embulk.parser.joni;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfigException;
import org.embulk.spi.TestPageBuilderReader;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.Newline;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestJoniParserPlugin
{
    private ConfigSource config;
    private JoniParserPlugin plugin;
    private TestPageBuilderReader.MockPageOutput output;

    @Before
    public void createResource()
    {
        config = config().set("type", "jsonpath");
        plugin = new JoniParserPlugin();
        recreatePageOutput();
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void checkDefaultValues()
    {
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
        assertEquals(false, task.getStopOnInvalidRecord());
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

    @Test // (expected = SchemaConfigException.class)
    public void checkNamedCaptureColumnNotFound()
    {
        ConfigSource config2 = Exec.newConfigSource()
                .set("columns", ImmutableList.of(
                        ImmutableMap.of(
                                "name", "hoge",
                                "type", "string")))
                .set("format", "(?<no_capture_name>a*)");

        config2.loadConfig(JoniParserPlugin.PluginTask.class);
        try {
            transaction(config2, fileInput(""));
        }
        catch (Throwable t) {
            assertTrue(t instanceof SchemaConfigException);
        }
    }

    @Test(expected = org.embulk.config.ConfigException.class)
    public void checkNoNamedCapturingGroupRegex()
            throws Exception
    {
        ConfigSource config2 = Exec.newConfigSource()
                .set("columns", ImmutableList.of(
                        ImmutableMap.of(
                                "name", "hoge",
                                "type", "string")))
                .set("format", "no named capturing group regex");

        config2.loadConfig(JoniParserPlugin.PluginTask.class);
        transaction(config2, fileInput(""));
    }

    @Test(expected = org.joni.exception.SyntaxException.class)
    public void checkInvalidRegexSyntax()
            throws Exception
    {
        ConfigSource config2 = Exec.newConfigSource()
                .set("columns", ImmutableList.of(
                        ImmutableMap.of(
                                "name", "hoge",
                                "type", "string")))
                .set("format", "(?<invalid_regex");

        config2.loadConfig(JoniParserPlugin.PluginTask.class);
        transaction(config2, fileInput(""));
    }

    private void transaction(ConfigSource config, final FileInput input)
    {
        plugin.transaction(config, new ParserPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema schema)
            {
                plugin.run(taskSource, schema, input, output);
            }
        });
    }

    private void recreatePageOutput()
    {
        output = new TestPageBuilderReader.MockPageOutput();
    }

    private FileInput fileInput(String... lines)
            throws Exception
    {
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line).append("\n");
        }

        ByteArrayInputStream in = new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));
        return new InputStreamFileInput(runtime.getBufferAllocator(), provider(in));
    }

    private InputStreamFileInput.IteratorProvider provider(InputStream... inputStreams)
            throws IOException
    {
        return new InputStreamFileInput.IteratorProvider(
                ImmutableList.copyOf(inputStreams));
    }

    private ConfigSource config()
    {
        return runtime.getExec().newConfigSource();
    }
}
