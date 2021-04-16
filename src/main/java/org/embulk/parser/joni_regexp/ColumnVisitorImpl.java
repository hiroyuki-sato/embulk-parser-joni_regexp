package org.embulk.parser.joni_regexp;

import java.time.Instant;
import org.embulk.parser.joni_regexp.JoniRegexpParserPlugin.PluginTask;
import org.embulk.parser.joni_regexp.JoniRegexpParserPlugin.TypecastColumnOption;

import org.embulk.spi.Column;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.core.MessageTypeException;
import org.msgpack.value.Value;

public class ColumnVisitorImpl implements ColumnVisitor
{
    protected final PluginTask task;
    protected final Schema schema;
    protected final PageBuilder pageBuilder;
    protected final TimestampFormatter[] timestampParsers;
    protected final Boolean[] autoTypecasts;

    protected Value value;

    public ColumnVisitorImpl(PluginTask task, Schema schema, PageBuilder pageBuilder, TimestampFormatter[] timestampParsers)
    {
        this.task = task;
        this.schema = schema;
        this.pageBuilder = pageBuilder;
        this.timestampParsers = timestampParsers.clone();
        this.autoTypecasts = new Boolean[schema.size()];
        buildAutoTypecasts();
    }

    private void buildAutoTypecasts()
    {
        for (Column column : schema.getColumns()) {
            this.autoTypecasts[column.getIndex()] = task.getDefaultTypecast();
        }

        SchemaConfig schemaConfig = task.getColumns();

        for (ColumnConfig columnConfig : schemaConfig.getColumns()) {
            TypecastColumnOption columnOption = columnConfig.getOption().loadConfig(TypecastColumnOption.class);
            Boolean autoTypecast = columnOption.getTypecast().orElse(task.getDefaultTypecast());
            Column column = schema.lookupColumn(columnConfig.getName());
            this.autoTypecasts[column.getIndex()] = autoTypecast;
        }
    }

    public void setValue(Value value)
    {
        this.value = value;
    }

    @Override
    public void booleanColumn(Column column)
    {
        if (isNil(value)) {
            pageBuilder.setNull(column);
        }
        else {
            try {
                boolean booleanValue = autoTypecasts[column.getIndex()] ? ColumnCaster.asBoolean(value) : value.asBooleanValue().getBoolean();
                pageBuilder.setBoolean(column, booleanValue);
            }
            catch (MessageTypeException e) {
                throw new JsonRecordValidateException(String.format("failed to get \"%s\" as Boolean", value), e);
            }
        }
    }

    @Override
    public void longColumn(Column column)
    {
        if (isNil(value)) {
            pageBuilder.setNull(column);
        }
        else {
            try {
                long longValue = autoTypecasts[column.getIndex()] ? ColumnCaster.asLong(value) : value.asIntegerValue().toLong();
                pageBuilder.setLong(column, longValue);
            }
            catch (MessageTypeException e) {
                throw new JsonRecordValidateException(String.format("failed to get \"%s\" as Long", value), e);
            }
        }
    }

    @Override
    public void doubleColumn(Column column)
    {
        if (isNil(value)) {
            pageBuilder.setNull(column);
        }
        else {
            try {
                double doubleValue = autoTypecasts[column.getIndex()] ? ColumnCaster.asDouble(value) : value.asFloatValue().toDouble();
                pageBuilder.setDouble(column, doubleValue);
            }
            catch (MessageTypeException e) {
                throw new JsonRecordValidateException(String.format("failed get \"%s\" as Double", value), e);
            }
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        if (isNil(value)) {
            pageBuilder.setNull(column);
        }
        else {
            try {
                String string = autoTypecasts[column.getIndex()] ? ColumnCaster.asString(value) : value.asStringValue().toString();
                pageBuilder.setString(column, string);
            }
            catch (MessageTypeException e) {
                throw new JsonRecordValidateException(String.format("failed to get \"%s\" as String", value), e);
            }
        }
    }

    @Override
    public void timestampColumn(Column column)
    {
        if (isNil(value)) {
            pageBuilder.setNull(column);
        }
        else {
            try {
                Instant timestamp = ColumnCaster.asTimestamp(value, timestampParsers[column.getIndex()]);
                pageBuilder.setTimestamp(column, timestamp);
            }
            catch (MessageTypeException e) {
                throw new JsonRecordValidateException(String.format("failed to get \"%s\" as Timestamp", value), e);
            }
        }
    }

    @Override
    public void jsonColumn(Column column)
    {
        if (isNil(value)) {
            pageBuilder.setNull(column);
        }
        else {
            try {
                JsonParser parser = new JsonParser();
                Value json = parser.parse(value.toString());
                pageBuilder.setJson(column, json);
            }
            catch (MessageTypeException e) {
                throw new JsonRecordValidateException(String.format("failed to get \"%s\" as Json", value), e);
            }
        }
    }

    protected boolean isNil(Value v)
    {
        return v == null || v.isNilValue();
    }
}
