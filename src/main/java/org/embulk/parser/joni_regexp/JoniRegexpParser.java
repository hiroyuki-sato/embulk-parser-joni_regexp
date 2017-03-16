package org.embulk.parser.joni_regexp;

import org.embulk.spi.FileInput;
import org.embulk.config.Task;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.spi.util.FileInputInputStream;
import org.embulk.spi.util.LineDecoder;
import org.embulk.spi.util.Newline;

import java.io.IOException;
import java.nio.charset.Charset;

import java.nio.ByteBuffer;

public class JoniRegexpParser
{
    private final int INITIAL_BUFFER_SIZE = 1024*32;
    private final int MINIMUL_BUFFER_SIZE = 1024;
    private final Newline newline;
    private ByteBuffer buffer;
    private final FileInputInputStream inputStream;
    private int newlineSearchPosition;

    private final byte CR = 13;
    private final byte LF = 10;

//    private final byte[] CR = {13};
//    private final byte[] LF = {10};
    private final byte[] CRLF = {13,10};


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
        this.newline = task.getNewline();

    }

    public boolean nextFie(){
        boolean has = inputStream.nextFile();
// TODO
//        if (has && charset.equals(UTF_8)) {
//            skipBom();
//        }
        return has;

    }

    public byte[] poll()
    {
        try {
//            return reader.readLine();

        } catch (IOException ex) {
            // unexpected
            throw new RuntimeException(ex);
        }
    }

    private int newLinePos(){
        int pos = newlineSearchPosition;
        ;
        int limit = buffer.limit();
        char firstChar = newline.getFirstCharCode();

        for(; pos < limit ; pos++ ){
            byte a = buffer.get(pos);

            if (newline == Newline.CRLF && a == CR) {
                if( pos+1 >= limit ){
                    return -1;
                }
                pos++;
                a = buffer.get(pos);
                if( a == LF ) {
                    return pos;
                }
            }
            else if ( (newline == newline.CR && a == CR) || (newline == newline.LF && a == LF) ){
                newlineSearchPosition = pos + 1;
                return pos;
            }
        }
        newlineSearchPosition = pos;
        return -1;
    }

/*
    private byte[] newLineByte(Newline newlineChar)
    {
        byte[] newlineByte = new byte[0];
        switch(newline) {
            case CRLF:
                break;
            case LF:
                newlineByte = LF
                break;
            case CR:
                newlineByte = CR;
                break;
            default:
                newlineByte = CRLF;
        }
        return newlineByte;

    }
*/

}
