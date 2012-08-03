package testAggregate;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;

import org.mortbay.log.Log;

public class AggregateSpout extends BaseRichSpout {
    /**
	 * 
	 */
	private static final long serialVersionUID = 5960861931292103064L;
	
	SpoutOutputCollector collector;
	
	int counter = 0;
	
    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(2000);
        
        double nextValue = (double)(int)(Math.random() * 11);
        
        Log.info("Sending - Time: " + counter + " Value: " + nextValue);
        collector.emit(new Values(counter,nextValue));
        counter++;
    }        

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(true,new Fields("time","value"));
    }
    
}