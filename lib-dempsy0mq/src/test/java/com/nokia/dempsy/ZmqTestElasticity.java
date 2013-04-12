package com.nokia.dempsy;

import org.junit.Test;
import org.slf4j.LoggerFactory;

public class ZmqTestElasticity extends TestElasticity
{
   static 
   {
      logger = LoggerFactory.getLogger(ZmqTestElasticity.class);
      transports = new String[][] { { "testDempsy/Transport-ZmqActx.xml" } };
   }
   
   @Test
   public void testForProfiler() throws Throwable { super.testForProfiler(); }

   @Test
   public void testNumberCountDropOneAndReAdd() throws Throwable { super.testNumberCountDropOneAndReAdd(); }

   @Test
   public void testNumberCountAddOneThenDrop() throws Throwable { super.testNumberCountAddOneThenDrop(); }
}
