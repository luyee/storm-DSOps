package testAggregate;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class Test {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("s", new AggregateSpout(), 1);
		builder.setBolt("a1", new AggregateTupleBolt(), 1).shuffleGrouping("s");
		builder.setBolt("a2", new AggregateReceiverBolt(), 1).shuffleGrouping("a1");

		Config conf = new Config();
//		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(60000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

}
