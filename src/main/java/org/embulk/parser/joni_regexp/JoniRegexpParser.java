package org.embulk.parser.joni_regexp;

import org.embulk.spi.FileInput;
import org.embulk.config.Task;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.spi.util.FileInputInputStream;
import org.embulk.spi.util.LineDecoder;
import org.embulk.spi.util.Newline;

import java.nio.charset.Charset;

import java.nio.ByteBuffer;

public class JoniRegexpParser
{
    private final int INITIAL_BUFFER_SIZE = 1024*32;
    private final int MINIMUL_BUFFER_SIZE = 1024;
    private final Newline newline;
    private ByteBuffer buffer;
    private final FileInputInputStream inputStream;

    public static interface DecoderTask
            extends Task
    {
        @Config("charset")
        @ConfigDefault("\"utf-8\"")
        public Charset getCharset();

        @Config("newline")
        @ConfigDefault("\"CRLF\"")
        public Newline getNewline();
    }


    public JoniRegexpParser(FileInput in,DecoderTask task){

        this.inputStream = new FileInputInputStream(in);
        this.buffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);

    }

    public boolean nextFie(){
        boolean has = inputStream.nextFile();
// TODO
//        if (has && charset.equals(UTF_8)) {
//            skipBom();
//        }
        return has;

    }


}
