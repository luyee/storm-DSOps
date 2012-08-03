package Aggregate;

import java.util.List;

import backtype.storm.tuple.Tuple;

public interface IAggregateTimeBolt<T extends Number> {

	public boolean processTuple(Tuple t,T time,List<Object> output);
	
}
