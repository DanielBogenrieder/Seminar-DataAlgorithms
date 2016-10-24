package test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.knime.base.data.filter.column.FilterColumnTable;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.DataValueColumnFilter;

/**
 * This is the model implementation of Kmeans.
 * 
 *
 * @author Simon Schmid
 */
public class KmeansNodeModel extends NodeModel {

	// the logger instance
	private static final NodeLogger logger = NodeLogger.getLogger(KmeansNodeModel.class);

	/**
	 * the settings key which is used to retrieve and store the settings (from
	 * the dialog or from a settings file) (package visibility to be usable from
	 * the dialog).
	 */
	static final String CFGKEY_K = "K";
	static final String CFGKEY_maxIterations = "maxNrIterations";
	static final String CFGKEY_columnfilter = "columnfilter";
	static final String CFGKEY_distMetric = "distMetric";
	static final String CFGKEY_parallelizedBool = "parallelizedBool";
	static final String CFGKEY_useInitialization = "useInitialization";

	/** initial default count value. */
	static final int DEFAULT_K = 3;
	static final int DEFAULT_maxIterations = 100;
	static DataValueColumnFilter COLUMN_FILTER = new DataValueColumnFilter(DoubleValue.class);
	static final String DFEAULT_distMetric = "Euclidean";
	static final boolean DEFAULT_parallelizedBool = true;
	static final boolean DEFAULT_useInitialization = true;

	private final SettingsModelIntegerBounded m_k = new SettingsModelIntegerBounded(KmeansNodeModel.CFGKEY_K,
			KmeansNodeModel.DEFAULT_K, Integer.MIN_VALUE, Integer.MAX_VALUE);

	private final SettingsModelIntegerBounded m_maxIterations = new SettingsModelIntegerBounded(
			KmeansNodeModel.CFGKEY_maxIterations, KmeansNodeModel.DEFAULT_maxIterations, Integer.MIN_VALUE,
			Integer.MAX_VALUE);

	private final SettingsModelFilterString m_columnfilter = new SettingsModelFilterString(CFGKEY_columnfilter);

	private final SettingsModelString m_distMetric = new SettingsModelString(CFGKEY_distMetric, DFEAULT_distMetric);

	private final SettingsModelBoolean m_parallelizedBool = new SettingsModelBoolean(CFGKEY_parallelizedBool,
			DEFAULT_parallelizedBool);

	private final SettingsModelBoolean m_useInitialization = new SettingsModelBoolean(CFGKEY_useInitialization,
			DEFAULT_useInitialization);

	/**
	 * Constructor for the node model.
	 */
	protected KmeansNodeModel() {
		// TODO one incoming port and one outgoing port is assumed
		super(1, 2);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
			throws Exception {
		BufferedDataTable inTable = inData[0];

		// cloumn filter
		List<String> colNames = m_columnfilter.getIncludeList();
		int[] indices = new int[colNames.size()];
		for (int i = 0; i < colNames.size(); i++) {
			indices[i] = inTable.getSpec().findColumnIndex(colNames.get(i));
		}

		int numRows = (int) inTable.size();
		int numColumns = indices.length;
		int k = m_k.getIntValue();
		double[][] centroids = new double[k][numColumns];
		double[][] initCentroids = new double[k][numColumns];
		FilterColumnTable fctable = new FilterColumnTable(inData[0], indices);

		// create 1. output table
		DataRow rowTemp = fctable.iterator().next();
		int inDataSize = rowTemp.getNumCells();
		DataColumnSpec[] allColSpecs = new DataColumnSpec[inDataSize + 1];
		String[] columnNames = fctable.getDataTableSpec().getColumnNames();
		for (int i = 0; i < inDataSize; i++) {
			allColSpecs[i] = new DataColumnSpecCreator(columnNames[i], DoubleCell.TYPE).createSpec();
		}
		allColSpecs[inDataSize] = new DataColumnSpecCreator("Cluster", StringCell.TYPE).createSpec();
		DataTableSpec outputSpec = new DataTableSpec(allColSpecs);

		BufferedDataContainer container = exec.createDataContainer(outputSpec);

		// create second output table
		DataColumnSpec[] allColSpecs2 = new DataColumnSpec[inDataSize + 1];
		for (int i = 0; i < inDataSize; i++) {
			allColSpecs2[i] = new DataColumnSpecCreator(columnNames[i], DoubleCell.TYPE).createSpec();
		}
		allColSpecs2[inDataSize] = new DataColumnSpecCreator("Cluster", StringCell.TYPE).createSpec();
		DataTableSpec outputSpec2 = new DataTableSpec(allColSpecs2);

		BufferedDataContainer container2 = exec.createDataContainer(outputSpec2);

		// write data in array
		double[][] tableAll = new double[inData[0].getRowCount()][numColumns];
		int counter = 0;
		for (DataRow row : fctable) {
			for (int i = 0; i < numColumns; i++) {
				tableAll[counter][i] = ((DoubleValue) row.getCell(i)).getDoubleValue();
			}
			counter++;
		}

		int[] clusterOfRow = new int[numRows];
		boolean[] taken = new boolean[numRows];
		/*
		 * loop for test issues
		 */
		double[][] table = tableAll;

		/*
		 * initialize centroids created
		 */
		/*
		 * choose first k rows as centroids
		 */
		if (!m_useInitialization.getBooleanValue()) {
			for (int i = 0; i < k; i++) {
				for (int j = 0; j < numColumns; j++) {
					centroids[i][j] = table[i][j];
				}
			}
			/*
			 * k-means++ initialization
			 */
		} else {
			Arrays.fill(taken, false);
			Random rand = new Random();
			int i = rand.nextInt(numRows);
			for (int j = 0; j < numColumns; j++) {
				centroids[0][j] = table[i][j];
			}
			taken[i] = true;
			double[] minDistOfRow = new double[numRows];
			for (i = 1; i < k; i++) {
				double distSqSum = 0;
				for (int index = 0; index < numRows; index++) {
					double actualDistance = distance(m_distMetric.getStringValue(), table[index], centroids[0]);
					for (int countToK = 1; countToK < k; countToK++) {
						double distance = distance(m_distMetric.getStringValue(), table[index], centroids[countToK]);
						if (distance < actualDistance) {
							actualDistance = distance;
						}
					}
					minDistOfRow[index] = actualDistance;
					distSqSum += actualDistance;
				}
				double r = rand.nextDouble() * distSqSum;
				double sum = 0;
				for (int index = 0; index < numRows; index++) {
					if (!taken[index]) {
						sum += minDistOfRow[index];
						if (sum >= r) {
							taken[index] = true;
							for (int j = 0; j < numColumns; j++) {
								centroids[i][j] = table[index][j];
							}
							break;
						}
					}
				}
			}
		}

		initCentroids = centroids;

		/*
		 * main loop k-means algorithm
		 */

		/*
		 * parallelized
		 */
		if (m_parallelizedBool.getBooleanValue()) {
			ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
			List<Callable<Object>> list = new ArrayList<Callable<Object>>();
			int maxNumThreads = Runtime.getRuntime().availableProcessors();
			for (int i = 0; i < m_maxIterations.getIntValue(); i++) {
				exec.checkCanceled();
				exec.setProgress((double) i / (double) m_maxIterations.getIntValue(), "Iteration " + i);
				double[] nrRowsInCluster = new double[k];
				double[][] clusterSum = new double[k][numColumns];
				double[][] allNrRowsInCluster = new double[maxNumThreads][k];
				double[][][] allClusterSum = new double[maxNumThreads][k][numColumns];
				int interval = numRows / maxNumThreads;
				for (int j = 0; j < maxNumThreads - 1; j++) {
					RunnableComputeCluster task = new RunnableComputeCluster(centroids, m_distMetric.getStringValue(),
							numColumns, table, clusterOfRow, k, allNrRowsInCluster[j], allClusterSum[j], (j * interval),
							(j + 1) * interval - 1);
					list.add(task);
				}
				RunnableComputeCluster task = new RunnableComputeCluster(centroids, m_distMetric.getStringValue(),
						numColumns, table, clusterOfRow, k, allNrRowsInCluster[maxNumThreads - 1],
						allClusterSum[maxNumThreads - 1], ((maxNumThreads - 1) * interval), numRows - 1);
				list.add(task);

				try {
					executor.invokeAll(list);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				for (int j1 = 0; j1 < maxNumThreads; j1++) {
					for (int j2 = 0; j2 < k; j2++) {
						nrRowsInCluster[j2] += allNrRowsInCluster[j1][j2];
						for (int j3 = 0; j3 < numColumns; j3++) {
							clusterSum[j2][j3] += allClusterSum[j1][j2][j3];
						}
					}
				}
				list.clear();

				// 1. compute new centroids
				// 2. check if the algorithm is finished
				// 3. break if it is
				double[][] centroidsTemp = new double[k][numColumns];
				double epsilon = Math.ulp(1.0);
				boolean br = true;
				for (int m = 0; m < k; m++) {
					for (int n = 0; n < numColumns; n++) {
						// 1.
						if (nrRowsInCluster[m] == 0) {
							centroidsTemp[m][n] = centroids[m][n];
						} else {
							centroidsTemp[m][n] = clusterSum[m][n] / nrRowsInCluster[m];
						}
						// 2.
						if (Math.abs(centroidsTemp[m][n] - centroids[m][n]) >= epsilon) {
							br = false;
						}
					}
				}
				// 3.
				if (br) {
					break;
				}

				// if the algorithm is not finished, iterate again
				// with
				// the
				// new centroids
				centroids = centroidsTemp;
			}

			/*
			 * non-parallelized
			 */
		} else {
			for (int i = 0; i < m_maxIterations.getIntValue(); i++) {
				exec.checkCanceled();
				exec.setProgress((double) i / (double) m_maxIterations.getIntValue(), "Iteration " + i);
				double[] nrRowsInCluster = new double[k];
				double[][] clusterSum = new double[k][numColumns];
				// compute new cluster
				for (int l = 0; l < numRows; l++) {
					double actualDistance = distance(m_distMetric.getStringValue(), table[l], centroids[0]);
					int actualCluster = 0;
					for (int countToK = 1; countToK < k; countToK++) {
						double distance = distance(m_distMetric.getStringValue(), table[l], centroids[countToK]);
						if (distance < actualDistance) {
							actualDistance = distance;
							actualCluster = countToK;
						}
					}
					clusterOfRow[l] = actualCluster;
					nrRowsInCluster[actualCluster]++;
					for (int j = 0; j < numColumns; j++) {
						clusterSum[actualCluster][j] += table[l][j];
					}
				}

				// 1. compute new centroids
				// 2. check if the algorithm is finished
				// 3. break if it is
				double[][] centroidsTemp = new double[k][numColumns];
				double epsilon = Math.ulp(1.0);
				boolean br = true;
				for (int m = 0; m < k; m++) {
					for (int n = 0; n < numColumns; n++) {
						// 1.
						if (nrRowsInCluster[m] == 0) {
							centroidsTemp[m][n] = centroids[m][n];
						} else {
							centroidsTemp[m][n] = clusterSum[m][n] / nrRowsInCluster[m];
						}
						// 2.
						if (Math.abs(centroidsTemp[m][n] - centroids[m][n]) >= epsilon) {
							br = false;
						}
					}
				}

				// 3.
				if (br) {
					break;
				}

				// if the algorithm is not finished, iterate again
				// with
				// the
				// new centroids
				centroids = centroidsTemp;
			}
		}

		/*
		 * create 1. output table
		 */
		for (int i = 0; i < numRows; i++) {
			// RowKey key = r.getKey();
			RowKey key = RowKey.createRowKey(i);
			DataCell[] cells = new DataCell[inDataSize + 1];
			// add input table to output
			for (int j = 0; j < inDataSize; j++) {
				cells[j] = new DoubleCell(table[i][j]);
			}
			// add cluster column to output
			cells[inDataSize] = new StringCell("cluster_" + clusterOfRow[i]);
			DataRow rowAdd = new DefaultRow(key, cells);
			container.addRowToTable(rowAdd);
		}
		container.close();

		BufferedDataTable out1 = container.getTable();

		/*
		 * create 2. output table
		 */
		for (int i = 0; i < k; i++)

		{
			RowKey key = RowKey.createRowKey(i);
			DataCell[] cells = new DataCell[inDataSize + 1];
			// add input table to output
			for (int j = 0; j < inDataSize; j++) {
				cells[j] = new DoubleCell(centroids[i][j]);
			}
			// add cluster column to output
			cells[inDataSize] = new StringCell("cluster_0" + i);
			DataRow rowAdd = new DefaultRow(key, cells);
			container2.addRowToTable(rowAdd);
		}

		container2.close();

		BufferedDataTable out2 = container2.getTable();

		return new BufferedDataTable[] { out1, out2 };

	}

	/*
	 * helper functions
	 */
	private double distance(String metric, double[] actualRow, double[] centroids) {
		// euclidean distance
		double result = 0;
		if (metric.equals("Euclidean")) {
			for (int i = 0; i < actualRow.length; i++) {
				result += (actualRow[i] - centroids[i]) * (actualRow[i] - centroids[i]);
			}
			// result = Math.sqrt(result);
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {

		ArrayList<DataColumnSpec> list = new ArrayList<DataColumnSpec>();

		for (DataColumnSpec spec : inSpecs[0]) {
			if (COLUMN_FILTER.includeColumn(spec)) {
				list.add(spec);
			}
		}

		String[] columnNames = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			columnNames[i] = list.get(i).getName();
		}

		m_columnfilter.setIncludeList(columnNames);

		return new DataTableSpec[] { null, null };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {

		// TODO save user settings to the config object.

		m_k.saveSettingsTo(settings);
		m_maxIterations.saveSettingsTo(settings);
		m_columnfilter.saveSettingsTo(settings);
		m_distMetric.saveSettingsTo(settings);
		m_parallelizedBool.saveSettingsTo(settings);
		m_useInitialization.saveSettingsTo(settings);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {

		// TODO load (valid) settings from the config object.
		// It can be safely assumed that the settings are valided by the
		// method below.

		m_k.loadSettingsFrom(settings);
		m_maxIterations.loadSettingsFrom(settings);
		m_columnfilter.loadSettingsFrom(settings);
		m_distMetric.loadSettingsFrom(settings);
		m_parallelizedBool.loadSettingsFrom(settings);
		m_useInitialization.loadSettingsFrom(settings);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {

		// TODO check if the settings could be applied to our model
		// e.g. if the count is in a certain range (which is ensured by the
		// SettingsModel).
		// Do not actually set any values of any member variables.

		m_k.validateSettings(settings);
		m_maxIterations.validateSettings(settings);
		m_columnfilter.validateSettings(settings);
		m_distMetric.validateSettings(settings);
		m_parallelizedBool.validateSettings(settings);
		m_useInitialization.validateSettings(settings);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(final File internDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {

		// TODO load internal data.
		// Everything handed to output ports is loaded automatically (data
		// returned by the execute method, models loaded in loadModelContent,
		// and user settings set through loadSettingsFrom - is all taken care
		// of). Load here only the other internals that need to be restored
		// (e.g. data used by the views).

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(final File internDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {

		// TODO save internal models.
		// Everything written to output ports is saved automatically (data
		// returned by the execute method, models saved in the saveModelContent,
		// and user settings saved through saveSettingsTo - is all taken care
		// of). Save here only the other internals that need to be preserved
		// (e.g. data used by the views).

	}

}
