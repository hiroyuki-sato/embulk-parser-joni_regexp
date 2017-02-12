package org.embulk.parser.joni;

import org.embulk.spi.DataException;

public class JsonRecordValidateException
        extends DataException
{
    public JsonRecordValidateException(String message)
    {
        super(message);
    }

    public JsonRecordValidateException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public JsonRecordValidateException(Throwable cause)
    {
        super(cause);
    }
}
