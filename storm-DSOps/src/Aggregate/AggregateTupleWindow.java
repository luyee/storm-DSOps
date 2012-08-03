package Aggregate;

import java.util.LinkedList;
import java.util.List;
import backtype.storm.tuple.Tuple;

public class AggregateTupleWindow implements
		IAggregateTupleWindow, IAggregateTupleBolt {

	List<IAggregateFunction> functions;
	LinkedList<Tuple> tuples;

	int size;
	int advance;

	public AggregateTupleWindow(List<IAggregateFunction> functions, int size,
			int advance) {

		this.functions = functions;
		tuples = new LinkedList<Tuple>();

		this.size = size;
		this.advance = advance;

	}

	@Override
	public void addTuple(Tuple t) {
		for (IAggregateFunction function : functions) {
			function.increment(t);
		}
		tuples.addLast(t);
	}

	@Override
	public boolean isWindowFull() {
		return tuples.size()==size;
	}

	@Override
	public void getWindowResult(List<Object> t) {
		for (IAggregateFunction function : functions) {
			function.finalize(t);
		}
	}

	@Override
	public void shift() {

		// Check the window is full?
		
		for (int i = 0; i < advance; i++) {
			tuples.removeFirst();
		}

	}

	@Override
	public boolean processTuple(Tuple t,List<Object> output) {

		boolean result = false;

		addTuple(t);

		if (isWindowFull()) {
			getWindowResult(output);
			shift();
			result = true;
		}

		return result;
	}

}
