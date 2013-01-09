package eu.stratosphere.nephele.streaming.latency;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.streaming.profiling.ProfilingValue;
import eu.stratosphere.nephele.streaming.profiling.ProfilingValueStatistic;

public class ProfilingValueStatisticTest {

	private ProfilingValueStatistic valueStatistic;

	@Before
	public void setup() {
		this.valueStatistic = new ProfilingValueStatistic(7);
	}

	@Test
	public void testValueSorted() {
		this.valueStatistic.addValue(this.createProfilingValue(1, 1));
		this.valueStatistic.addValue(this.createProfilingValue(2, 3));
		this.valueStatistic.addValue(this.createProfilingValue(3, 7));
		this.valueStatistic.addValue(this.createProfilingValue(4, 8));
		this.valueStatistic.addValue(this.createProfilingValue(5, 21));
		this.valueStatistic.addValue(this.createProfilingValue(6, 35));
		this.valueStatistic.addValue(this.createProfilingValue(7, 41));

		assertTrue(this.valueStatistic.getMedianValue() == 8);
		assertTrue(this.valueStatistic.getMinValue() == 1);
		assertTrue(this.valueStatistic.getMaxValue() == 41);
		assertTrue(this.valueStatistic.getArithmeticMean() - 16.5714 < 0.0001);
	}

	private ProfilingValue createProfilingValue(long timestamp, double value) {
		return new ProfilingValue(value, timestamp);
	}

	@Test
	public void testAddValueUnsorted() {
		this.valueStatistic.addValue(this.createProfilingValue(1, 7));
		this.valueStatistic.addValue(this.createProfilingValue(2, 15));
		this.valueStatistic.addValue(this.createProfilingValue(3, 13));
		this.valueStatistic.addValue(this.createProfilingValue(4, 1));
		this.valueStatistic.addValue(this.createProfilingValue(5, 5));
		this.valueStatistic.addValue(this.createProfilingValue(6, 7.5));
		this.valueStatistic.addValue(this.createProfilingValue(7, 8));

		assertTrue(this.valueStatistic.getMedianValue() == 7.5);
		assertTrue(this.valueStatistic.getMinValue() == 1);
		assertTrue(this.valueStatistic.getMaxValue() == 15);
		assertTrue(this.valueStatistic.getArithmeticMean() - 8.0714 < 0.0001);
	}

	@Test
	public void testAddValueReverseSorted() {
		this.valueStatistic.addValue(this.createProfilingValue(1, 18));
		this.valueStatistic.addValue(this.createProfilingValue(2, 15));
		this.valueStatistic.addValue(this.createProfilingValue(3, 13));
		this.valueStatistic.addValue(this.createProfilingValue(4, 10));
		this.valueStatistic.addValue(this.createProfilingValue(5, 9));
		this.valueStatistic.addValue(this.createProfilingValue(6, 8));
		this.valueStatistic.addValue(this.createProfilingValue(7, 7));

		assertTrue(this.valueStatistic.getMedianValue() == 10);
		assertTrue(this.valueStatistic.getMinValue() == 7);
		assertTrue(this.valueStatistic.getMaxValue() == 18);
		assertTrue(this.valueStatistic.getArithmeticMean() - 11.4285 < 0.0001);
	}

	@Test
	public void testAddValueOverfullUnsorted() {
		this.valueStatistic.addValue(this.createProfilingValue(1, 7));
		this.valueStatistic.addValue(this.createProfilingValue(2, 15));
		this.valueStatistic.addValue(this.createProfilingValue(3, 13));
		this.valueStatistic.addValue(this.createProfilingValue(4, 1));
		this.valueStatistic.addValue(this.createProfilingValue(5, 7.5));
		this.valueStatistic.addValue(this.createProfilingValue(6, 5));
		this.valueStatistic.addValue(this.createProfilingValue(7, 18));
		this.valueStatistic.addValue(this.createProfilingValue(8, 13));
		this.valueStatistic.addValue(this.createProfilingValue(9, 10));
		this.valueStatistic.addValue(this.createProfilingValue(10, 8));

		assertTrue(this.valueStatistic.getMedianValue() == 8);
		assertTrue(this.valueStatistic.getMinValue() == 1);
		assertTrue(this.valueStatistic.getMaxValue() == 18);
		assertTrue(this.valueStatistic.getArithmeticMean() - 8.9285 < 0.0001);
	}

	@Test
	public void testAddValueOverfullSorted() {
		this.valueStatistic.addValue(this.createProfilingValue(1, 1));
		this.valueStatistic.addValue(this.createProfilingValue(2, 2));
		this.valueStatistic.addValue(this.createProfilingValue(3, 3));
		this.valueStatistic.addValue(this.createProfilingValue(4, 4));
		this.valueStatistic.addValue(this.createProfilingValue(5, 5));
		this.valueStatistic.addValue(this.createProfilingValue(6, 6));
		this.valueStatistic.addValue(this.createProfilingValue(7, 7));
		this.valueStatistic.addValue(this.createProfilingValue(8, 8));
		this.valueStatistic.addValue(this.createProfilingValue(9, 9));
		this.valueStatistic.addValue(this.createProfilingValue(10, 10));

		assertTrue(this.valueStatistic.getMedianValue() == 7);
		assertTrue(this.valueStatistic.getMinValue() == 4);
		assertTrue(this.valueStatistic.getMaxValue() == 10);
		assertTrue(this.valueStatistic.getArithmeticMean() == 7);
	}

	@Test
	public void testGetMedianUnderfull() {
		this.valueStatistic.addValue(this.createProfilingValue(1, 18));
		this.valueStatistic.addValue(this.createProfilingValue(2, 15));
		assertTrue(this.valueStatistic.getMedianValue() == 18);

		this.valueStatistic.addValue(this.createProfilingValue(3, 17));
		assertTrue(this.valueStatistic.getMedianValue() == 17);
	}

	@Test
	public void testGetMinMaxUnderfull() {
		this.valueStatistic.addValue(this.createProfilingValue(1, 18));
		this.valueStatistic.addValue(this.createProfilingValue(2, 15));
		assertTrue(this.valueStatistic.getMinValue() == 15);
		assertTrue(this.valueStatistic.getMaxValue() == 18);

		this.valueStatistic.addValue(this.createProfilingValue(3, 17));
		assertTrue(this.valueStatistic.getMinValue() == 15);
		assertTrue(this.valueStatistic.getMaxValue() == 18);

	}

	@Test
	public void testGetArithmeticMeanUnderfull() {
		this.valueStatistic.addValue(this.createProfilingValue(1, 18));
		this.valueStatistic.addValue(this.createProfilingValue(2, 15));
		assertTrue(this.valueStatistic.getArithmeticMean() == 16.5);

		this.valueStatistic.addValue(this.createProfilingValue(3, 17));
		assertTrue(this.valueStatistic.getArithmeticMean() - 16.6666 < 0.0001);
	}
}
