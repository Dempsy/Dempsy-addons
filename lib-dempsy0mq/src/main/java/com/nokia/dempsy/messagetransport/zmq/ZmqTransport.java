package com.nokia.dempsy.messagetransport.zmq;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import com.nokia.dempsy.executor.DefaultDempsyExecutor;
import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.OverflowHandler;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.messagetransport.SenderFactory;
import com.nokia.dempsy.messagetransport.Transport;
import com.nokia.dempsy.messagetransport.tcp.TcpDestination;
import com.nokia.dempsy.messagetransport.util.ForwardedReceiver;
import com.nokia.dempsy.messagetransport.util.ForwardingSender;
import com.nokia.dempsy.messagetransport.util.ForwardingSender.Enqueued;
import com.nokia.dempsy.messagetransport.util.ForwardingSenderFactory;
import com.nokia.dempsy.messagetransport.util.ReceiverIndexedDestination;
import com.nokia.dempsy.messagetransport.util.SenderConnection;
import com.nokia.dempsy.messagetransport.util.Server;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.util.MessageBufferInput;
import com.nokia.dempsy.util.MessageBufferOutput;

public class ZmqTransport implements Transport
{
   private String thisNodeDescription = null;
   private ZMQ.Context context;
   private boolean blocking = false;
   private long numberOfServers = 0;
   
   private synchronized void deref()
   {
      numberOfServers--;
      if (numberOfServers == 0)
         context.term();
   }
   
   private synchronized void ref()
   {
      if (numberOfServers == 0)
         context = ZMQ.context(1);
      numberOfServers++;
   }
   
   private final Server server = new Server(logger,false)
   {
      private InterruptibleZmqSocket socket = null;
      private boolean socketIsOpen = false;
      private TcpDestination destination = null;
      
      @Override
      protected ReceiverIndexedDestination makeDestination(ReceiverIndexedDestination modelDestination, int receiverIndex)
      {
         return new TcpDestination((TcpDestination)modelDestination,receiverIndex);
      }

      // never called if manageIndividualClients (passed to the Server constructor) is false
      @Override
      protected Object accept() { return null; }

      // This needs to prepare the condition for the server to shutdown. We need to interrupt the thread.
      @Override
      protected void closeServer() 
      {
         closeClient(null,false);
      }

      // called instead of closeServer when manageIndividualClients is true
      @Override
      protected synchronized void closeClient(Object acceptReturn, boolean fromClientThread) 
      {
         if (!socketIsOpen)
            return;
         
         if (socket != null)
         {
            socket.interrupt();
            
            if (fromClientThread)
            {
               socket.close();
               socketIsOpen = false;
               deref();
            }
         }
      }

      @Override
      protected synchronized ReceiverIndexedDestination getAndBindDestination() throws MessageTransportException
      {
         ref();
         
         socket = new InterruptibleZmqSocket();
         int port = bindToEphemeralPort(socket);
         socketIsOpen = true;
         if (destination != null)
            return destination;
         
         if (port <= 0)
            throw new MessageTransportException("Cannot create a Desintation from a Zmq Reveiver without starting it first.");
         
         destination = TcpDestination.createNewDestinationForThisHost(port, false);
         return destination;
      }

      @Override
      protected void readNextMessage(Object acceptReturn, ReceivedMessage messageToFill) throws EOFException, IOException
      {
         // mark as failed by default.
         final byte[] rawMessage = socket.recv();
         if (rawMessage == null) // should only be possible on an interrupt
         {
            messageToFill.message = null; // mark failed.
            return;
         }
         @SuppressWarnings("resource") // mb has no real close.
         final MessageBufferInput mb = new MessageBufferInput(rawMessage);
         messageToFill.message = mb;
         messageToFill.receiverIndex = mb.read();
      }

      @Override
      protected String getClientDescription(Object acceptReturn) { return "all incoming 0mq connections to " + thisNodeDescription + "," + destination; }

   };
   
   private final Map<Destination,SenderConnection> connections = new HashMap<Destination, SenderConnection>();

   private OverflowHandler overflowHandler = null;
   private boolean failFast = false;
   private long maxNumberOfQueuedOutbound = 10000;

   private static final Logger logger = LoggerFactory.getLogger(ZmqTransport.class);
   private static final AtomicLong interruptibleSocketSequence = new AtomicLong(0);
   private static final byte[] interruptMessage = new byte[] { 0 };

   private final class InterruptibleZmqSocket
   {
      private final ZMQ.Socket interrupt;
      private final ZMQ.Socket socket;
      private final long sequence;
      private boolean interrupted = false;
      
      public InterruptibleZmqSocket()
      {
         sequence = interruptibleSocketSequence.getAndIncrement();
         interrupt = context.socket(ZMQ.PULL);
         interrupt.bind("inproc://" + sequence);
         socket = context.socket(ZMQ.PULL);
      }
      
      public final void interrupt()
      {
         ZMQ.Socket isock = null;
         try
         {
            isock = context.socket(ZMQ.PUSH);
            isock.connect("inproc://" + sequence);
            isock.send(interruptMessage,0);
         }
         finally
         {
            if (isock != null)
               isock.close();
         }
      }
      
      public final void close() { socket.close(); interrupt.close(); interrupted = true; }
      public final void bind(String bstr) { socket.bind(bstr); }
      public final boolean isInterrupted() 
      {
         return (interrupted ? true : 
            (interrupted = (interrupt.recv(ZMQ.DONTWAIT) != null)));
      }
      public final byte[] recv() 
      {
         byte[] ret = socket.recv(ZMQ.DONTWAIT);
         while (ret == null)
         {
            // check interrupt
            if (isInterrupted())
               return null;
            Thread.yield();
            ret = socket.recv(ZMQ.DONTWAIT);
         }
         return ret;
      }
   }
   
   @Override
   public void setOverflowHandler(OverflowHandler overflowHandler)
   {
      this.overflowHandler = overflowHandler;
   }
   
   public void setFailFast(boolean failFast) { this.failFast = failFast; }
   
   /**
    * <p>The {@link ForwardingSender} sends data from a dedicated thread and reads from a queue. This will set
    * the maximum number of pending sends. When the maximum number of pending sends is reached, the oldest
    * data will be discarded.</p>
    * 
    * <p>Setting the value to -1 will allow the queue to be unbounded. The default is 10000.</p>
    */
   public void setMaxNumberOfQueuedOutbound(long maxNumberOfQueuedOutbound)
   {
      this.maxNumberOfQueuedOutbound = maxNumberOfQueuedOutbound;
   }

   public void setBlocking(boolean blocking) { this.blocking = blocking; }


   @Override
   public SenderFactory createOutbound(final DempsyExecutor executor, final StatsCollector statsCollector, final String desc) throws MessageTransportException
   {
      return new ForwardingSenderFactory(connections,statsCollector,desc)
      {
         private final long maxNumberOfQueuedOutbound = ZmqTransport.this.maxNumberOfQueuedOutbound;
         
         @Override
         protected ReceiverIndexedDestination makeBaseDestination(ReceiverIndexedDestination destination)
         {
            return ((TcpDestination)destination).baseDestination();
         }

         @Override
         protected SenderConnection makeNewSenderConnection(final ReceiverIndexedDestination baseDestination)
         {
            return new SenderConnection(baseDestination.toString(),this, blocking, maxNumberOfQueuedOutbound, false, logger)
            {
               final TcpDestination destination = (TcpDestination)baseDestination;
               ZMQ.Socket socket = null;
               
               { ref(); }
               
               @Override
               protected void doSend(final Enqueued enqueued, final boolean batch) throws MessageTransportException
               {
                  if (socket == null)
                  {
                     socket = context.socket(ZMQ.PUSH);
                     socket.connect ("tcp://" + destination.getInetAddress().getHostAddress() + ":" + destination.getPort());
                  }

                  final MessageBufferOutput message = enqueued.message;
                  final byte[] messageBytes = message.getBuffer();
                  // See the SenderFactory.prepareMessage where this byte is reserved
                  messageBytes[0] = (byte)(enqueued.getReceiverIndex());
                  
                  if (!socket.send(messageBytes, 0, message.getPosition(), 0))
                     throw new MessageTransportException("Send failed. Who knows why?");
               }

               @Override protected void flush() { }

               @Override
               protected void close()
               {
                  if (socket != null)
                  {
                     try { socket.close(); } catch (Throwable zmqe) { logger.error("Failed closing Zmq Sender to " + SafeString.valueOf(destination)); }
                     socket = null;
                  }
               }

               @Override
               protected void cleanup() { deref(); }
            };
         }
         
      };
   }
   
   @Override
   public Receiver createInbound(DempsyExecutor executor, String desc) throws MessageTransportException
   {
      thisNodeDescription = desc;
      boolean receiverShouldStopExecutor = false;
      if (executor == null)
      {
         DefaultDempsyExecutor defexecutor = new DefaultDempsyExecutor();
         defexecutor.setCoresFactor(1.0);
         defexecutor.setAdditionalThreads(1);
         defexecutor.setMaxNumberOfQueuedLimitedTasks(10000);
         defexecutor.setUnlimited(true);
         defexecutor.setBlocking(blocking);
         defexecutor.start();
         executor = defexecutor;
         receiverShouldStopExecutor = true;
      }
      
      ForwardedReceiver receiver = new ForwardedReceiver(server,executor,failFast, receiverShouldStopExecutor);
      receiver.setOverflowHandler(overflowHandler);
      return receiver;
   }
   
   private final static int bindToEphemeralPort(InterruptibleZmqSocket socket) throws MessageTransportException
   {
      // find an unused ephemeral port
      int port = -1;
      for (boolean done = false; !done;)
      {
         port = -1;
         try
         {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(InetAddress.getLocalHost(), 0);
            ServerSocket serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true); // this allows the server port to be bound to even if it's in TIME_WAIT
            serverSocket.bind(inetSocketAddress);
            port = serverSocket.getLocalPort();
            serverSocket.close();
            socket.bind("tcp://*:" + port);
            done = true;
         }
         catch (IOException e)
         {
            logger.error("Failed attempting to open a 0mq server socket on " + port,e);
         }
         catch (RuntimeException e)
         {
            logger.error("Failed attempting to open a 0mq server socket on " + port,e);
         }
      }
      
      if (port <= 0)
         throw new MessageTransportException("Couldn't set the tcp port for the Zmq Receiver.");
      
      return port;
   }
}
