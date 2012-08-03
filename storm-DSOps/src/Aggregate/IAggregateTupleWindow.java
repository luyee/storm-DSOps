package Aggregate;

import backtype.storm.tuple.Tuple;

public interface IAggregateTupleWindow extends IAggregateWindow {

	public void addTuple(Tuple t);
	
	public boolean isWindowFull();

	public void shift();
	
}
