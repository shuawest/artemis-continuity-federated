/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.continuity.core;

import javax.jms.Connection;

import org.apache.activemq.continuity.ContinuityTestBase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FederatedQueueTest extends ContinuityTestBase {
 
  private static final Logger log = LoggerFactory.getLogger(FederatedQueueTest.class);

  @Test
  public void bridgeCommandTest() throws Exception {
    ServerContext serverCtx1 = createServerContext("broker1-noplugin.xml", "primary-server", "myuser", "mypass");
    serverCtx1.getServer().start();

    ServerContext serverCtx2 = createServerContext("broker2-noplugin.xml", "backup-server", "myuser", "mypass");
    serverCtx2.getServer().start();

    Thread.sleep(2000);

    log.debug("\n\nProducing test messages on broker1\n\n");
    produceJmsMessages("tcp://localhost:61616", "myuser", "mypass", "example2-durable", "test message", 100);

    log.debug("\n\nStarting consumer for test message on broker1\n\n");
    JmsMessageListenerStub handler1 = new JmsMessageListenerStub("broker1");
    Connection conn1 = startJmsConsumer("tcp://localhost:61616", "myuser", "mypass", "example2-durable", handler1);
    
    log.debug("\n\nStarting consumer for test message on broker2\n\n");
    JmsMessageListenerStub handler2 = new JmsMessageListenerStub("broker2");
    Connection conn2 = startJmsConsumer("tcp://localhost:61617", "myuser", "mypass", "example2-durable", handler2);
    
    Thread.sleep(35L);
    conn1.stop();
    conn1.stop();

    Thread.sleep(1000);  
    serverCtx1.getServer().asyncStop(()->{ log.debug("\n\nserver1 stopped\n\n"); });

    serverCtx1.getServer().start();
    Thread.sleep(2000L);

    log.debug("\n\n{} Messages consumed off broker1:\n{}", handler1.getMessageCount(), handler1.getMessagesAsString());
    log.debug("\n\n{} Messages consumed off broker2:\n{}", handler2.getMessageCount(), handler2.getMessagesAsString());

    log.debug("\n\nShutting down\n\n");

    serverCtx1.getServer().asyncStop(()->{ log.debug("\n\nserver1 stopped again\n\n"); });
    conn2.stop();
    conn2.close();
    serverCtx2.getServer().asyncStop(()->{ log.debug("\n\nserver2 stopped\n\n"); });
  }

}
