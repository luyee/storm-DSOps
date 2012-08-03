package Aggregate;

import java.util.LinkedList;
import java.util.List;
import backtype.storm.tuple.Tuple;

public class AggregateTimeWindow<T extends Number> implements
		IAggregateTimeWindow<T>, IAggregateTimeBolt<T> {

	class WindowElement {

		Tuple t;
		T time;

		public WindowElement(Tuple t, T time) {
			this.t = t;
			this.time = time;
		}

	}

	List<IAggregateFunction> functions;
	LinkedList<WindowElement> tuples;

	T size;
	T advance;

	public AggregateTimeWindow(List<IAggregateFunction> functions, T size,
			T advance) {

		this.functions = functions;
		tuples = new LinkedList<WindowElement>();

		this.size = size;
		this.advance = advance;

	}

	@Override
	public void addTuple(Tuple t, T time) {
		for (IAggregateFunction function : functions) {
			function.increment(t);
		}
		tuples.addLast(new WindowElement(t, time));
	}

	@Override
	public boolean isWindowFull(T time) {
		if (tuples.isEmpty())
			return false;
		return time.doubleValue() - tuples.getFirst().time.doubleValue() >= size
				.doubleValue();
	}

	@Override
	public void getWindowResult(List<Object> t) {
		for (IAggregateFunction function : functions) {
			function.finalize(t);
		}
	}

	@Override
	public void shift(T time) {

		while (tuples.size() > 0 && isWindowFull(time)) {
			T firstTime = tuples.getFirst().time;
			double nextStart = firstTime.doubleValue() + advance.doubleValue();
			while (tuples.size() > 0
					&& tuples.getFirst().time.doubleValue() < nextStart) {
				for (IAggregateFunction function : functions) {
					function.decrement(tuples.getFirst().t);
				}
				tuples.removeFirst();
			}
		}

	}

	@Override
	public boolean processTuple(Tuple t, T time,List<Object> output) {

		boolean result = false;
		
		if (isWindowFull(time)) {
			getWindowResult(output);
			shift(time);
			result = true;
		}

		addTuple(t, time);

		return result;
	}

}
