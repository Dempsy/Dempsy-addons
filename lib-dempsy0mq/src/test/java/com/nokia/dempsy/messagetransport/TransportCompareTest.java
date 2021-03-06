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

package com.nokia.dempsy.messagetransport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.executor.DefaultDempsyExecutor;
import com.nokia.dempsy.message.MessageBufferInput;
import com.nokia.dempsy.message.MessageBufferOutput;
import com.nokia.dempsy.messagetransport.tcp.TcpTransport;
import com.nokia.dempsy.messagetransport.zmq.ZmqTransport;
import com.nokia.dempsy.monitoring.basic.BasicStatsCollector;

public class TransportCompareTest
{
   static Logger logger = LoggerFactory.getLogger(TransportCompareTest.class);
   
   @Test
   public void testCompareTcpZmq() throws Throwable
   {
      //==================================================================
      // First create the messages we will be sending.
      //==================================================================
      
      // random number of messages of random length.
      final int baseNumberOfMessages = 1000000; // 1 million
      final int numberOfMessagesVariablility = 10000; // 10 thousand
      
      final int baseMessageLength = 200;
      final int messageLengthVariability = 100;
      
      Random random = new Random();
      final int totalMessages = baseNumberOfMessages + (random.nextInt(numberOfMessagesVariablility / 2) - (numberOfMessagesVariablility / 2));
      final byte[][] messageSource = new byte[totalMessages][];
      long totalBytes = 0;
      for (int i = 0; i < totalMessages; i++)
      {
         final int nextMessageLength = baseMessageLength + (random.nextInt(messageLengthVariability / 2) - (messageLengthVariability / 2));
         totalBytes += nextMessageLength;
         messageSource[i] = new byte[nextMessageLength];
         random.nextBytes(messageSource[i]);
      }
      
      System.out.println("Testing 0mq");
      for (int i = 0; i < 3; i++)
      {
         ZmqTransport zmq = new ZmqTransport();
         zmq.setMaxNumberOfQueuedOutbound(-1);
         long time = timeMessageSending(zmq,false,messageSource);
         System.out.format("Message rate: %,d msg/sec. %,d bytes/sec.%n",((totalMessages * 1000) / time), ((totalBytes * 1000) / time));
      }
      System.out.println("Testing 0mq Blocking");
      for (int i = 0; i < 3; i++)
      {
         ZmqTransport zmq = new ZmqTransport();
         zmq.setBlocking(true);
         zmq.setMaxNumberOfQueuedOutbound(10000);
         long time = timeMessageSending(zmq,true,messageSource);
         System.out.format("Message rate: %,d msg/sec. %,d bytes/sec.%n",((totalMessages * 1000) / time), ((totalBytes * 1000) / time));
      }
      System.out.println("Testing Tcp");
      for (int i = 0; i < 3; i++)
      {
         TcpTransport tcp = new TcpTransport();
         tcp.setBatchOutgoingMessages(false);
         tcp.setMaxNumberOfQueuedOutbound(-1);
         long time = timeMessageSending(tcp,false,messageSource);
         System.out.format("Message rate: %,d msg/sec. %,d bytes/sec.%n",((totalMessages * 1000) / time), ((totalBytes * 1000) / time));
      }
      System.out.println("Testing Tcp Blocking");
      for (int i = 0; i < 3; i++)
      {
         TcpTransport tcp = new TcpTransport();
         tcp.setBatchOutgoingMessages(false);
         tcp.setMaxNumberOfQueuedOutbound(10000);
         tcp.setBlocking(true);
         long time = timeMessageSending(tcp,true,messageSource);
         System.out.format("Message rate: %,d msg/sec. %,d bytes/sec.%n",((totalMessages * 1000) / time), ((totalBytes * 1000) / time));
      }
      System.out.println("Testing Tcp batched");
      for (int i = 0; i < 3; i++)
      {
         TcpTransport tcp = new TcpTransport();
         tcp.setBatchOutgoingMessages(true);
         tcp.setMaxNumberOfQueuedOutbound(-1);
         long time = timeMessageSending(tcp,false,messageSource);
         System.out.format("Message rate: %,d msg/sec. %,d bytes/sec.%n",((totalMessages * 1000) / time), ((totalBytes * 1000) / time));
      }
      System.out.println("Testing Tcp batched and blocking");
      for (int i = 0; i < 3; i++)
      {
         TcpTransport tcp = new TcpTransport();
         tcp.setBatchOutgoingMessages(true);
         tcp.setMaxNumberOfQueuedOutbound(10000);
         tcp.setBlocking(true);
         long time = timeMessageSending(tcp,true,messageSource);
         System.out.format("Message rate: %,d msg/sec. %,d bytes/sec.%n",((totalMessages * 1000) / time), ((totalBytes * 1000) / time));
      }
      System.out.println("Finished with test.");
      //==================================================================
   }
   
   private long timeMessageSending(final Transport transport, boolean blocking, final byte[][] messages) throws Throwable
   {
      DefaultDempsyExecutor inboundExecutor = new DefaultDempsyExecutor(1,10000);
      if (blocking)
         inboundExecutor.setBlocking(true);
      else
         inboundExecutor.setUnlimited(true);
      inboundExecutor.start();
      
      final BasicStatsCollector statsCollector = new BasicStatsCollector();
      final CountDownLatch startingLine = new CountDownLatch(1);
      final CountDownLatch finishLine = new CountDownLatch(1);
      final String sendingSideDescription = "Sender side";
      final String receiveSideDescription = "Receive side";
      final SenderFactory senderFactory = transport.createOutbound(null, statsCollector, sendingSideDescription);
      final Receiver receiver = transport.createInbound(inboundExecutor, receiveSideDescription);
      final Destination destination = receiver.getDestination();
      
      final byte[][] receivedMessages = new byte[messages.length][];
      Listener listener = new Listener()
      {
         final int totalMessages = receivedMessages.length;
         int cur = 0;
         
         @Override public void transportShuttingDown() { }
         @Override public boolean onMessage(MessageBufferInput messageBytes, boolean failFast)  
         {
            receivedMessages[cur++] = messageBytes.readByteArray();
            if (cur == totalMessages)
               finishLine.countDown();
            return true;
         }
      };
      receiver.setListener(listener);
      receiver.start();

      final AtomicBoolean failed = new AtomicBoolean(false); 
   
      Thread sendingThread = new Thread(new Runnable()
      {
         Sender sender = senderFactory.getSender(destination);
         
         @Override public void run()
         {
            try
            {
               startingLine.await();
               
               final int size = messages.length;
               for (int i = 0; i < size; i++)
               {
                  final MessageBufferOutput msg = senderFactory.prepareMessage();
                  msg.write(messages[i]);
                  sender.send(msg);
               }
            }
            catch (Throwable th) { failed.set(true); }
         }
      }, sendingSideDescription);
      sendingThread.start();
      
      Thread.sleep(5000); // wait until everything is ready.

      long startTime = System.currentTimeMillis();
      startingLine.countDown(); // GO!
      
      finishLine.await();
      long totalTime = System.currentTimeMillis() - startTime;
      
      senderFactory.shutdown();
      receiver.shutdown();
      inboundExecutor.shutdown();
      
      assertFalse(failed.get());
      
      // check the results.
      for (int i = 0; i < messages.length; i++)
         assertTrue(Arrays.equals(messages[i],receivedMessages[i]));
      
//      System.out.println("Inbound high water mark is " + inboundExecutor.getHighWaterMark());
      
      return totalTime;
   }
   
}
