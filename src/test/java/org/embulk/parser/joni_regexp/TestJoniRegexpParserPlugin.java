package org.embulk.parser.joni_regexp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.TypeModule;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.spi.SchemaConfigException;
import org.embulk.spi.TestPageBuilderReader;
import org.embulk.spi.type.Type;
import org.embulk.util.file.InputStreamFileInput;
import org.embulk.spi.util.Pages;
import org.embulk.util.text.Newline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.Value;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertTrue;
import static org.msgpack.value.ValueFactory.newArray;
import static org.msgpack.value.ValueFactory.newFloat;
import static org.msgpack.value.ValueFactory.newInteger;
import static org.msgpack.value.ValueFactory.newMap;
import static org.msgpack.value.ValueFactory.newString;

public class TestJoniRegexpParserPlugin
{
    private ConfigSource config;
    private JoniRegexpParserPlugin plugin;
    private TestPageBuilderReader.MockPageOutput output;

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
            .builder()
            .addDefaultModules()
            .addModule(new TypeModule())
            .build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    private static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    // TODO
//     private static final String RESOURCE_NAME_PREFIX = "org/embulk/parser/joni/";

    @Before
    public void createResource()
    {
        config = config().set("type", "joni_regexp");
        plugin = new JoniRegexpParserPlugin();
        recreatePageOutput();
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

//    TODO use TestingEmbulk
//    @Rule
//    public TestingEmbulk embulk = TestingEmbulk.builder().build();

    @Test
    public void basicApacheCombinedLogTest()
            throws Exception
    {
        // TODO Use TestingEmbulk
        //        ConfigSource config = embulk.loadYamlResource(RESOURCE_NAME_PREFIX+"apache.yml");

        SchemaConfig schema = schema(
                column("host", STRING), column("user", STRING),
                column("time", TIMESTAMP, config().set("format", "%d/%b/%Y:%H:%M:%S %z")),
                column("method", STRING), column("path", STRING),
                column("code", STRING), column("size", STRING), column("referer", STRING),
                column("agent", STRING));
        ConfigSource config = this.config.deepCopy().set("columns", schema)
                .set("format", "^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \\[(?<time>[^\\]]*)\\] \"(?<method>\\S+)(?: +(?<path>[^ ]*) +\\S*)?\" (?<code>[^ ]*) (?<size>[^ ]*)(?: \"(?<referer>[^\\\"]*)\" \"(?<agent>[^\\\"]*)\")?$");

//        config.loadConfig(JoniRegexpParserPlugin.PluginTask.class);

        transaction(config, fileInput(
                "224.126.227.109 - - [13/Feb/2017:20:04:52 +0900] \"GET /category/games HTTP/1.1\" 200 85 \"-\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11\"",
                "128.27.132.24 - bob [13/Feb/2017:20:04:53 +0900] \"GET /category/health HTTP/1.1\" 200 103 \"/category/electronics?from=20\" \"Mozilla/5.0 (Windows NT 6.0; rv:10.0.1) Gecko/20100101 Firefox/10.0.1\""
        ));

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            Timestamp time = (Timestamp) record[2];
            assertEquals("224.126.227.109", record[0]);
            assertEquals("-", record[1]);
            assertEquals(Instant.ofEpochSecond(1486983892L), time.getInstant());
            assertEquals("GET", record[3]);
            assertEquals("/category/games", record[4]);
//            assertEquals("HTTP/1.1", record[5]);
            assertEquals("200", record[5]);
            assertEquals("85", record[6]);
            assertEquals("-", record[7]);
            assertEquals("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11", record[8]);
        }
        {
            record = records.get(1);
            Timestamp time = (Timestamp) record[2];
            assertEquals("128.27.132.24", record[0]);
            assertEquals("bob", record[1]);
            assertEquals(Instant.ofEpochSecond(1486983893L), time.getInstant());
            assertEquals("GET", record[3]);
            assertEquals("/category/health", record[4]);
//            assertEquals("HTTP/1.1", record[5]);
            assertEquals("200", record[5]);
            assertEquals("103", record[6]);
            assertEquals("/category/electronics?from=20", record[7]);
            assertEquals("Mozilla/5.0 (Windows NT 6.0; rv:10.0.1) Gecko/20100101 Firefox/10.0.1", record[8]);
        }
    }

    @Test
    public void basicApacheCombinedLogTestEnableStopOnInvalidRecord()
            throws Exception
    {
        // TODO Use TestingEmbulk
        //        ConfigSource config = embulk.loadYamlResource(RESOURCE_NAME_PREFIX+"apache.yml");

        SchemaConfig schema = schema(
                column("host", STRING), column("user", STRING),
                column("time", TIMESTAMP, config().set("format", "%d/%b/%Y:%H:%M:%S %z")),
                column("method", STRING), column("path", STRING),
                column("code", STRING), column("size", STRING), column("referer", STRING),
                column("agent", STRING));
        ConfigSource config = this.config.deepCopy().set("columns", schema)
                .set("format", "^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \\[(?<time>[^\\]]*)\\] \"(?<method>\\S+)(?: +(?<path>[^ ]*) +\\S*)?\" (?<code>[^ ]*) (?<size>[^ ]*)(?: \"(?<referer>[^\\\"]*)\" \"(?<agent>[^\\\"]*)\")?$")
                .set("stop_on_invalid_record", false);

//        config.loadConfig(JoniRegexpParserPlugin.PluginTask.class);

        transaction(config, fileInput(
                "224.126.227.109 - - [13/Feb/2017:20:04:52 +0900] \"GET /category/games HTTP/1.1\" 200 85 \"-\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11\"",
                "invalid_record1",
                "128.27.132.24 - bob [13/Feb/2017:20:04:53 +0900] \"GET /category/health HTTP/1.1\" 200 103 \"/category/electronics?from=20\" \"Mozilla/5.0 (Windows NT 6.0; rv:10.0.1) Gecko/20100101 Firefox/10.0.1\"",
                "invalid_record2"
        ));

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            Timestamp time = (Timestamp) record[2];
            assertEquals("224.126.227.109", record[0]);
            assertEquals("-", record[1]);
            assertEquals(Instant.ofEpochSecond(1486983892L), time.getInstant());
            assertEquals("GET", record[3]);
            assertEquals("/category/games", record[4]);
//            assertEquals("HTTP/1.1", record[5]);
            assertEquals("200", record[5]);
            assertEquals("85", record[6]);
            assertEquals("-", record[7]);
            assertEquals("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11", record[8]);
        }
        {
            record = records.get(1);
            Timestamp time = (Timestamp) record[2];
            assertEquals("128.27.132.24", record[0]);
            assertEquals("bob", record[1]);
            assertEquals(Instant.ofEpochSecond(1486983893L), time.getInstant());
            assertEquals("GET", record[3]);
            assertEquals("/category/health", record[4]);
            assertEquals("200", record[5]);
            assertEquals("103", record[6]);
            assertEquals("/category/electronics?from=20", record[7]);
            assertEquals("Mozilla/5.0 (Windows NT 6.0; rv:10.0.1) Gecko/20100101 Firefox/10.0.1", record[8]);
        }
    }

    @Test(expected = DataException.class)
    public void checkInvalidRecord()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("host", STRING), column("user", STRING),
                column("time", TIMESTAMP, config().set("format", "%d/%b/%Y:%H:%M:%S %z")),
                column("method", STRING), column("path", STRING),
                column("code", STRING), column("size", STRING), column("referer", STRING),
                column("agent", STRING));
        ConfigSource config = this.config.deepCopy().set("columns", schema)
                .set("format", "^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \\[(?<time>[^\\]]*)\\] \"(?<method>\\S+)(?: +(?<path>[^ ]*) +\\S*)?\" (?<code>[^ ]*) (?<size>[^ ]*)(?: \"(?<referer>[^\\\"]*)\" \"(?<agent>[^\\\"]*)\")?$")
                .set("stop_on_invalid_record", true);

        transaction(config, fileInput(
                "224.126.227.109 - - [13/Feb/2017:20:04:52 +0900] \"GET /category/games HTTP/1.1\" 200 85 \"-\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11\"",
                "invalid_record1",
                "128.27.132.24 - bob [13/Feb/2017:20:04:53 +0900] \"GET /category/health HTTP/1.1\" 200 103 \"/category/electronics?from=20\" \"Mozilla/5.0 (Windows NT 6.0; rv:10.0.1) Gecko/20100101 Firefox/10.0.1\"",
                "invalid_record2"
        ));
    }

    @Test
    public void checkLookahead()
            throws Exception
    {
        SchemaConfig schema = schema(
                column("string1", STRING), column("string2", STRING));

        ConfigSource config = this.config.deepCopy().set("columns", schema)
                .set("format", "^\"(?<string1>.*?)\"(?<!\\.) \"(?<string2>.*?)\"");

        transaction(config, fileInput(
                "\"This is a \\\"test\\\".\" \"This is a \\\"test\\\".\"]}"));

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals("This is a \\\"test\\\".", record[0]);
            assertEquals("This is a \\", record[1]);
        }
    }

    @Test(expected = DataException.class)
    public void checkInvalidFormat()
            throws Exception
    {

        SchemaConfig schema = schema(
                column("date", TIMESTAMP, config().set("format", "%H:%M:%S")));

        ConfigSource config = this.config.deepCopy().set("columns", schema)
                .set("format", "(?<date>\\S+)");

        transaction(config, fileInput(
                "2017-02-19 This is a test."));
    }

    @Test
    public void checkAllColumnTypes()
            throws Exception
    {

        SchemaConfig schema = schema(
                column("bool", BOOLEAN), column("string", STRING),
                column("time", TIMESTAMP, config().set("format", "%Y-%m-%d %H:%M:%S")),
                column("long", LONG), column("double", DOUBLE),
                column("json", JSON));

        ConfigSource config = this.config.deepCopy().set("columns", schema)
                .set("format", "^(?<bool>[^\t]*)\t(?<string>[^\t]*)\t(?<time>[^\t]*)\t*(?<long>[^\t]*)\t(?<double>[^\t]*)\t(?<json>[^\t]*)$");

        transaction(config, fileInput(
                "true\tマイケル・ジャクソン\t2009-6-25  00:00:00\t456789\t123.456\t{\"name\":\"Michael Jackson\",\"birth\":\"1958-8-29\",\"age\":50,\"Bad World Tour\":4.4,\"album\":[\"Got To Be There\",\"Ben\",\"Music & Me\"]}"));

        Value json = newMap(newString("name"), newString("Michael Jackson"),
                newString("birth"), newString("1958-8-29"),
                newString("age"), newInteger(50),
                newString("Bad World Tour"), newFloat(4.4),
                newString("album"), newArray(newString("Got To Be There"), newString("Ben"), newString("Music & Me")));
        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            Timestamp time = (Timestamp) record[2];
            assertEquals(true, record[0]);
            assertEquals("マイケル・ジャクソン", record[1]);
            assertEquals(Instant.ofEpochSecond(1245888000L), time.getInstant());
            assertEquals(456789L, record[3]);
            assertEquals(123.456, record[4]);
            assertEquals(json, record[5]);
        }
    }

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = config()
                .set("columns", Collections.unmodifiableList(Arrays.asList(
                        Collections.unmodifiableMap(new HashMap<String,String>(){
                            {
                                put("name","hoge");
                                put("type","string");
                            }}))))
                .set("format", "(?<name>a*)");

        JoniRegexpParserPlugin.PluginTask task = CONFIG_MAPPER.map(config, JoniRegexpParserPlugin.PluginTask.class);
        ;

        assertEquals(Charset.forName("utf-8"), task.getCharset());
        assertEquals(Newline.CRLF, task.getNewline());
        assertEquals("UTC", task.getDefaultTimeZoneId());
        assertEquals("%Y-%m-%d %H:%M:%S.%N %z", task.getDefaultTimestampFormat());
        assertEquals(false, task.getStopOnInvalidRecord());
        //assertEquals( true, task.getDefaultTypecast());

    }

    @Test(expected = ConfigException.class)
    public void checkColumnsRequired()
    {
        ConfigSource config = config()
                .set("format", "(?<name>a*)");

        //config.loadConfig(JoniRegexpParserPlugin.PluginTask.class);
        CONFIG_MAPPER.map(config, JoniRegexpParserPlugin.PluginTask.class);
    }

    @Test(expected = ConfigException.class)
    public void checkFormatRequired()
    {
        ConfigSource config = config()
                .set("columns", Collections.unmodifiableList(Arrays.asList(
                        Collections.unmodifiableMap(new HashMap<String,String>(){
                            {
                                put("name","hoge");
                                put("type","string");
                            }}))));

        //config.loadConfig(JoniRegexpParserPlugin.PluginTask.class);
        CONFIG_MAPPER.map(config, JoniRegexpParserPlugin.PluginTask.class);
    }

    @Test // (expected = SchemaConfigException.class)
    public void checkNamedCaptureColumnNotFound()
    {
        ConfigSource config2 = config()
                .set("columns", Collections.unmodifiableList(Arrays.asList(
                        Collections.unmodifiableMap(new HashMap<String,String>(){
                            {
                                put("name","hoge");
                                put("type","string");
                            }}))))
                .set("format", "(?<no_capture_name>a*)");

        //config2.loadConfig(JoniRegexpParserPlugin.PluginTask.class);
        CONFIG_MAPPER.map(config2, JoniRegexpParserPlugin.PluginTask.class);
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
        ConfigSource config2 = config()
                .set("columns", Collections.unmodifiableList(Arrays.asList(
                        Collections.unmodifiableMap(new HashMap<String,String>(){
                            {
                                put("name","hoge");
                                put("type","string");
                            }}))))
                .set("format", "no named capturing group regex");

        CONFIG_MAPPER.map(config2, JoniRegexpParserPlugin.PluginTask.class);
        //config2.loadConfig(JoniRegexpParserPlugin.PluginTask.class);
        transaction(config2, fileInput(""));
    }
    //@Test(expected = org.embulk.config.ConfigException.class)
    @Test(expected = org.joni.exception.ValueException.class)
    public void checkInvalidRegexSyntax()
            throws Exception
    {

        Map<String,String> opt = Collections.unmodifiableMap(new HashMap<String,String>(){
        {
            put("name","hoge");
            put("type","string");
        }});


        ConfigSource config2 = config()
                .set("columns", Collections.unmodifiableList(Arrays.asList(
                        Collections.unmodifiableMap(new HashMap<String,String>(){
                            {
                                put("name","hoge");
                                put("type","string");
                            }}))))
                .set("format", "(?<invalid_regex");

        CONFIG_MAPPER.map(config2, JoniRegexpParserPlugin.PluginTask.class);
        //config2.loadConfig(JoniRegexpParserPlugin.PluginTask.class);
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

    private SchemaConfig schema(ColumnConfig... columns)
    {
        return new SchemaConfig(Lists.newArrayList(columns));
    }

    private ColumnConfig column(String name, Type type)
    {
        return column(name, type, config());
    }

    private ColumnConfig column(String name, Type type, ConfigSource option)
    {
        return new ColumnConfig(name, type, option);
    }

}
