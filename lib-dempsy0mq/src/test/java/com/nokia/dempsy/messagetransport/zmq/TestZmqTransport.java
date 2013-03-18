package com.nokia.dempsy.messagetransport.zmq;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import org.junit.Test;

import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.util.MessageBufferInput;
import com.nokia.dempsy.util.MessageBufferOutput;

public class TestZmqTransport
{
   public static final int baseTimeoutMillis = 20000;
   
   @Test
   public void testZmqServerStart() throws Throwable
   {
      ZmqTransport transport = new ZmqTransport();
      Receiver r = null;
      try
      {
         r = transport.createInbound(null,null);
         r.setListener(new Listener() { 
            @Override public void transportShuttingDown(){}
            @Override public boolean onMessage(MessageBufferInput messageBytes, boolean failFast) { return true; }
         });
         r.start();
      }
      finally
      {
         if (r != null) r.shutdown();
      }
   }
   
   @Test
   public void testZmqSendSimpleMessage() throws Throwable
   {
      ZmqTransport transport = new ZmqTransport();
      Receiver r = null;
      SenderFactory senderFactory = null;
      try
      {
         r = transport.createInbound(null,null);
      
         final AtomicReference<String> lastReceived = new AtomicReference<String>();
         r.setListener(new Listener()
         {
            @Override public void transportShuttingDown() { }

            @Override
            public boolean onMessage(MessageBufferInput message, boolean failFast) throws MessageTransportException
            {
               lastReceived.set(new String(message.readByteArray()));
               return true;
            }
         });
         r.start();

         Destination d = r.getDestination();

         senderFactory = transport.createOutbound(null, null, null);
         Sender s = senderFactory.getSender(d);

         final MessageBufferOutput msg = senderFactory.prepareMessage();
         msg.write("Hello".getBytes());
         s.send(msg);

         assertTrue(TestUtils.poll(baseTimeoutMillis, lastReceived, new TestUtils.Condition<AtomicReference<String>>()
         {
            @Override public boolean conditionMet(AtomicReference<String> o) { return "Hello".equals(o.get()); }
         }));
      }
      finally
      {
         if (r != null) r.shutdown();
         if (senderFactory != null) senderFactory.shutdown();
      }
   }

}
