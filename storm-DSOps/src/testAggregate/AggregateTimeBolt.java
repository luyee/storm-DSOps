package testAggregate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.mortbay.log.Log;

import Aggregate.AggregateTimeWindow;
import Aggregate.IAggregateFunction;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class AggregateTimeBolt implements IRichBolt {

	private static final long serialVersionUID = -2918800814680639790L;

	OutputCollector collector;

	AggregateTimeWindow<Integer> window;

	class AggFunction implements IAggregateFunction {

		double sum;
		double counter;

		public AggFunction() {
			this.sum = 0;
			this.counter = 0;
		}

		@Override
		public void increment(Tuple t) {
			this.sum += t.getDouble(1);
			this.counter++;
		}

		@Override
		public void decrement(Tuple t) {
			this.sum -= t.getDouble(1);
			this.counter--;
		}

		@Override
		public void finalize(List<Object> t) {
			t.set(1, this.sum / this.counter);
		}

	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		List<IAggregateFunction> functions = new ArrayList<IAggregateFunction>();
		functions.add(new AggFunction());
		window = new AggregateTimeWindow<Integer>(functions, 5, 2);
	}

	@Override
	public void execute(Tuple input) {

		int time = input.getInteger(0);

		List<Object> result = new ArrayList<Object>();
		result.add(0);
		result.add(0);
		
		boolean emit = window.processTuple(input, time, result);
		collector.ack(input);

		if (emit) {
			result.set(0, time);
			Log.info("Emitting - Time: " + result.get(0) + " Average: "
					+ result.get(1));
			collector.emit(input, result);
		}

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
