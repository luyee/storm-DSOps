package Aggregate;

import java.util.List;

import backtype.storm.tuple.Tuple;

public interface IAggregateFunction {

	public void increment(Tuple t);
	
	public void decrement(Tuple t);
	
	public void finalize(List<Object> t);
	
}
