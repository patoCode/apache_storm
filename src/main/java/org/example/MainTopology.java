package org.example;

import java.util.Arrays;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology {

  private static final String SPOUT = "IntegerSpout";
  private static final String BOLT = "MultiplierBolt";
  private static final String TOPOLOGY = "HelloTopology";


  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT, new IntegerSpout());
    builder.setBolt(BOLT, new MultiplierBolt()).shuffleGrouping(SPOUT);

    Config config = new Config();
    config.put("nimbus.host","localhost");
    config.put(Config.NIMBUS_THRIFT_PORT, 6627);
    config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
    config.put(Config.STORM_ZOOKEEPER_PORT,2181);

    config.put(Config.TOPOLOGY_WORKERS, 3);
    //config.setDebug(true);

    LocalCluster cluster = new LocalCluster();
    try {
      cluster.submitTopology(TOPOLOGY, config, builder.createTopology());
      Thread.sleep(5000);
    } catch(Exception e){
      System.out.println(e.getMessage().toString());
    } finally {
      cluster.shutdown();
    }
  }

}