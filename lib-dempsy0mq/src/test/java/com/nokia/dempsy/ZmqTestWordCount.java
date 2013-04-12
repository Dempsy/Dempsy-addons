package com.nokia.dempsy;

import org.junit.Test;
import org.slf4j.LoggerFactory;

public class ZmqTestWordCount extends TestWordCount
{
   static 
   {
      logger = LoggerFactory.getLogger(ZmqTestWordCount.class);
      transports = new String[][] { { "testDempsy/Transport-ZmqActx.xml" } };
   }
   
   @Test
   public void testWordCount() throws Throwable { super.testWordCount(); }
}
