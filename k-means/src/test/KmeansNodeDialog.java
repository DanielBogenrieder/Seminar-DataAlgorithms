package test;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * <code>NodeDialog</code> for the "Kmeans" Node.
 * 
 *
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author
 */
public class KmeansNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring Kmeans node dialog. This is just a suggestion to
	 * demonstrate possible default dialog components.
	 */
	protected KmeansNodeDialog() {
		super();
		addDialogComponent(new DialogComponentNumber(new SettingsModelIntegerBounded(KmeansNodeModel.CFGKEY_K,
				KmeansNodeModel.DEFAULT_K, Integer.MIN_VALUE, Integer.MAX_VALUE), "K:", /* step */ 1,
				/* componentwidth */ 5));

		addDialogComponent(new DialogComponentNumber(
				new SettingsModelIntegerBounded(KmeansNodeModel.CFGKEY_maxIterations,
						KmeansNodeModel.DEFAULT_maxIterations, Integer.MIN_VALUE, Integer.MAX_VALUE),
				"max number of iterations (the number of optimizations):", 1, 5));

		addDialogComponent(new DialogComponentStringSelection(
				new SettingsModelString(KmeansNodeModel.CFGKEY_distMetric, KmeansNodeModel.DFEAULT_distMetric),
				"Distance metric:", new String[] { "Euclidean", "Manhattan", "Chessboard" }));
		// addDialogComponent(new DialogComponentStringListSelection(
		// new SettingsModelStringArray(KmeansNodeModel.CFGKEY_distMetric,
		// KmeansNodeModel.DFEAULT_distMetric),
		// null, "Distance metric:", KmeansNodeModel.DFEAULT_distMetric[0]));

		addDialogComponent(new DialogComponentBoolean(new SettingsModelBoolean(KmeansNodeModel.CFGKEY_parallelizedBool, KmeansNodeModel.DEFAULT_parallelizedBool), "compute parallel k-means algorithm"));

		addDialogComponent(new DialogComponentBoolean(new SettingsModelBoolean(KmeansNodeModel.CFGKEY_useInitialization, KmeansNodeModel.DEFAULT_useInitialization), "use k-means++ initialization for better clustering"));
		
		addDialogComponent(
				new DialogComponentColumnFilter(new SettingsModelFilterString(KmeansNodeModel.CFGKEY_columnfilter), 0,
						true, KmeansNodeModel.COLUMN_FILTER, true));
		

		setDefaultTabTitle("K-Means Properties");
	}
}
