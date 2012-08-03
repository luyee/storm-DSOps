package testAggregate;

import java.util.Map;
import org.mortbay.log.Log;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class AggregateReceiverBolt implements IRichBolt {

	private static final long serialVersionUID = -2918800814680639790L;

	OutputCollector collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {

		collector.ack(input);
		Log.info("Received - Time: " + input.getInteger(0) + " Average: " + input.getDouble(1));
		
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time", "average"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
