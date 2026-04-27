package com.example.multiPipelineEtl.pipeline.query1;

import junit.framework.TestCase;

public class MongoDbQuery1PipelineTest extends TestCase {
    public void testParseLineParsesNasaFormatAndBytesDash() {
        ParsedLogRecord record = MongoDbQuery1Pipeline.parseLine(
            "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /images/launch-logo.gif HTTP/1.0\" 200 -"
        );

        assertNotNull(record);
        assertEquals("1995-08-01", record.getLogDate());
        assertEquals(200, record.getStatusCode());
        assertEquals(0L, record.getBytesTransferred());
    }

    public void testParseLineParsesAlternateTimestampFormat() {
        ParsedLogRecord record = MongoDbQuery1Pipeline.parseLine(
            "hostA - - [Tue Aug 01 00:00:01 1995] \"GET /index.html HTTP/1.0\" 404 321"
        );

        assertNotNull(record);
        assertEquals("1995-08-01", record.getLogDate());
        assertEquals(404, record.getStatusCode());
        assertEquals(321L, record.getBytesTransferred());
    }

    public void testParseLineReturnsNullForMalformedRecord() {
        ParsedLogRecord record = MongoDbQuery1Pipeline.parseLine("bad-line");
        assertNull(record);
    }
}
