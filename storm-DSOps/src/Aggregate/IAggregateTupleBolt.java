package Aggregate;

import java.util.List;

import backtype.storm.tuple.Tuple;

public interface IAggregateTupleBolt {

	public boolean processTuple(Tuple t,List<Object> output);
	
}
