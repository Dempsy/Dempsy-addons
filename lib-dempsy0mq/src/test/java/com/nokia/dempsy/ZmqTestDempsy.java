/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nokia.dempsy;

import org.junit.Test;
import org.slf4j.LoggerFactory;

public class ZmqTestDempsy extends TestDempsy
{
   static
   {
      logger = LoggerFactory.getLogger(ZmqTestDempsy.class);
      transports = new String[][] { { "testDempsy/Transport-ZmqActx.xml" } };
   }
   
   @Test
   public void testStartupShutdown() throws Throwable { super.testStartupShutdown();  }
   
   @Test
   public void testMpStartMethod() throws Throwable { super.testMpStartMethod(); }
   
   @Test
   public void testMessageThrough() throws Throwable { super.testMessageThrough(); }
   
   @Test
   public void testMessageThroughWithClusterFailure() throws Throwable { super.testMessageThroughWithClusterFailure(); }
   
   @Test
   public void testOutPutMessage() throws Throwable { super.testOutPutMessage(); }


   @Test
   public void testCronOutPutMessage() throws Throwable { super.testCronOutPutMessage(); }

   @Test
   public void testExplicitDesintationsStartup() throws Throwable { super.testExplicitDesintationsStartup(); }
   
}
