package org.embulk.parser.joni_regexp.cast;

import org.embulk.spi.DataException;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestDoubleCast
{
    @Test
    public void asBoolean()
    {
        try {
            DoubleCast.asBoolean(0.5);
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof DataException);
        }
    }

    @Test
    public void asLong()
    {
        assertEquals(0, DoubleCast.asLong(0.5));
    }

    @Test
    public void asDouble()
    {
        assertEquals(0.5, DoubleCast.asDouble(0.5), 0.0);
    }

    @Test
    public void asString()
    {
        assertEquals("0.5", DoubleCast.asString(0.5));
    }

    @Test
    public void asTimestamp()
    {
        Instant expected = Instant.ofEpochSecond(1, 500000000);
        assertEquals(expected, DoubleCast.asTimestamp(1.5));
    }
}
