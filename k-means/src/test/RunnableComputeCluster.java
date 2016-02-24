package test;

import java.util.concurrent.Callable;

public class RunnableComputeCluster implements Callable {

	String distMetric;
	double[][] centroids;
	int numColumns;
	double[][] table;
	int[] clusterOfRow;
	int k;
	double[] nrRowsInCluster;
	double[][] clusterSum;
	int rowStart;
	int rowEnd;

	public RunnableComputeCluster(double[][] centroids, String distMetric, int numColumns, double[][] table,
			int[] clusterOfRow, int k, double[] nrRowsInCluster, double[][] clusterSum, int rowStart, int rowEnd) {
		this.centroids = centroids;
		this.distMetric = distMetric;
		this.numColumns = numColumns;
		this.table = table;
		this.clusterOfRow = clusterOfRow;
		this.k = k;
		this.nrRowsInCluster = nrRowsInCluster;
		this.clusterSum = clusterSum;
		this.rowStart = rowStart;
		this.rowEnd = rowEnd;
	}

	private double distance(String metric, double[] actualRow, double[] centroids) {
		// euclidean distance
		double result = 0;
		if (metric.equals("Euclidean")) {
			for (int i = 0; i < actualRow.length; i++) {
				result += (actualRow[i] - centroids[i]) * (actualRow[i] - centroids[i]);
			}
		}

		if (metric.equals("Manhattan")) {
			for (int i = 0; i < actualRow.length; i++) {
				result += Math.abs((actualRow[i] - centroids[i]));
			}
		}

		if (metric.equals("Chessboard")) {
			for (int i = 0; i < actualRow.length; i++) {
				result = Math.max(Math.abs((actualRow[i] - centroids[i])), result);
			}
		}

		return result;
	}

	@Override
	public Object call() throws Exception {
		for (int index = rowStart; index <= rowEnd; index++) {
			double actualDistance = distance(distMetric, table[index], centroids[0]);
			int actualCluster = 0;
			for (int countToK = 1; countToK < k; countToK++) {
				double distance = distance(distMetric, table[index], centroids[countToK]);
				if (distance < actualDistance) {
					actualDistance = distance;
					actualCluster = countToK;
				}
			}
			clusterOfRow[index] = actualCluster;
			nrRowsInCluster[actualCluster]++;
			for (int j = 0; j < numColumns; j++) {
				clusterSum[actualCluster][j] += table[index][j];
			}
		}
		return null;
	}

}
