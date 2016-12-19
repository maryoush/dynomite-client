package com.hybris.poc;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.lb.AbstractTokenMapSupplier;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import com.netflix.dyno.queues.ShardSupplier;
import com.netflix.dyno.queues.redis.RedisQueues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Main {

    private final static Logger LOG = LoggerFactory.getLogger(Main.class);
    
    private static final String queueName = "test_queue";

    private static final String redisKeyPrefix = "testdynoqueues";

    private static final AtomicInteger atomicInteger = new AtomicInteger();

    private static final String RACK = "rack1";


    final static String json = "[" + //
            "{\"zone\":\"rack1\",\"hostname\":\"host1\",\"host\":\"127.0.0.1\",\"port\":\"8102\",\"token\":\"1383429731\"}," +//
            "{\"zone\":\"rack2\",\"hostname\":\"host2\",\"host\":\"127.0.0.2\",\"port\":\"8102\",\"token\":\"1383429731\"}," +//
            "{\"zone\":\"rack3\",\"hostname\":\"host3\",\"host\":\"127.0.0.3\",\"port\":\"8102\",\"token\":\"1383429731\"}" +//
            "]";

    final static HostSupplier customHostSupplier = new HostSupplier() {

        final List<Host> hosts = new ArrayList<Host>();

        @Override
        public Collection<Host> getHosts() {
            hosts.add(new Host("host1", "127.0.0.1", 8102, Host.Status.Up).setRack("rack1"));
            hosts.add(new Host("host2", "127.0.0.2", 8102, Host.Status.Up).setRack("rack2"));
            hosts.add(new Host("host3", "127.0.0.3", 8102, Host.Status.Up).setRack("rack3"));

            return hosts;
        }
    };


    private static TokenMapSupplier testTokenMapSupplier = new AbstractTokenMapSupplier() {


        @Override
        public String getTopologyJsonPayload(final Set<Host> activeHosts) {
            return json;
        }

        @Override
        public String getTopologyJsonPayload(String hostname) {
            return json;
        }
    };


    final static Set<String> allShards = customHostSupplier.getHosts().stream().map(host -> host.getRack().substring(
            host.getRack().length() - 2)).collect(Collectors.toSet());


    public static void main(String... args) throws Exception {
        ConnectionPoolConfigurationImpl connectionPoolConfiguration = new ConnectionPoolConfigurationImpl(RACK);
        connectionPoolConfiguration.withTokenSupplier(testTokenMapSupplier);
        connectionPoolConfiguration.setLocalRack(RACK);

        String shardName = allShards.iterator().next();

        ShardSupplier ss = new ShardSupplier() {

            @Override
            public Set<String> getQueueShards() {
                return allShards;
            }

            @Override
            public String getCurrentShard() {
                return shardName;
            }
        };

        DynoJedisClient dynoClient = new DynoJedisClient.Builder()  //
                .withApplicationName("appname")   //
                .withHostSupplier(customHostSupplier)  //
                .withCPConfig(connectionPoolConfiguration)//
                .build();

        RedisQueues rq = new RedisQueues(dynoClient, null, redisKeyPrefix, ss, 1_000, 1_000_000, 100);


        DynoQueue q  = rq.get("queue1");

        Message m1 = createMessage("1");
        Message m2 = createMessage("2");
        Message m3 = createMessage("3");
        Message m4 = createMessage("4");
        Message m5 = createMessage("5");
        Message m6 = createMessage("6");


        q.push(Collections.singletonList(m1));
        q.push(Collections.singletonList(m2));
        q.push(Collections.singletonList(m3));
        q.push(Collections.singletonList(m4));
        q.push(Collections.singletonList(m5));
        q.push(Collections.singletonList(m6));


        readMessages(q);
        readMessages(q);
        readMessages(q);



//
//        ScheduledExecutorService ex = Executors.newScheduledThreadPool(1);
//
//        ex.scheduleAtFixedRate(() ->
//        {
//            writeData(rq);
//
//        }, 1000, 5000, TimeUnit.MILLISECONDS);
//
//
//        ScheduledExecutorService ex2 = Executors.newScheduledThreadPool(1);
//
//        ex2.scheduleAtFixedRate(() ->
//        {
//            readData(rq);
//
//        }, 1000, 5000, TimeUnit.MILLISECONDS);
//
//
        LOG.info("\nPress ENTER to proceed.\n");
        System.in.read();

        dynoClient.stopClient();
    }

    private static void readMessages(DynoQueue q) throws InterruptedException {
        Thread.sleep(1000);


        List<Message> msgs =  q.pop(100,1000, TimeUnit.MILLISECONDS);

        msgs.forEach(m ->
                {
                    System.out.println(m);
                    System.out.println("Acked "+q.ack(m.getId())+" "+ m.getId());
                }
        );
    }

    static Message createMessage(String id)
    {
        return new Message(id+"-"+atomicInteger.incrementAndGet()+"-"+System.currentTimeMillis(),"hello");
    }


}