package Aggregate;

import backtype.storm.tuple.Tuple;

public interface IAggregateTimeWindow<T extends Number> extends IAggregateWindow {

	public void addTuple(Tuple t,T time);
	
	public boolean isWindowFull(T time);

	public void shift(T time);
	
}
