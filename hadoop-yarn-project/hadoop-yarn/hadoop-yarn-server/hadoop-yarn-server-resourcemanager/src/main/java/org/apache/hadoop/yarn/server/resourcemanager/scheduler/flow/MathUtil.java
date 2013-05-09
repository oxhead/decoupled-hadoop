/**
 * 
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.List;

/**
 * @author oxhead
 * 
 */
public class MathUtil {
	public static double calcuateMean(List<Double> numbers) {
		int sum = 0;
		for (Double n : numbers) {
			sum += n.doubleValue();
		}
		return (double) sum / numbers.size();
	}

	public static double calcuateStd(List<Double> numbers) {
		double mean = calcuateMean(numbers);
		double square_difference = 0;
		for (Double n : numbers) {
			square_difference += Math.pow(n - mean, 2);
		}
		return Math.sqrt(square_difference / numbers.size());
	}
}
