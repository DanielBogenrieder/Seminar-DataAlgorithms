package test;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "Kmeans" Node.
 * 
 *
 * @author 
 */
public class KmeansNodeFactory 
        extends NodeFactory<KmeansNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public KmeansNodeModel createNodeModel() {
        return new KmeansNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<KmeansNodeModel> createNodeView(final int viewIndex,
            final KmeansNodeModel nodeModel) {
        return new KmeansNodeView(nodeModel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new KmeansNodeDialog();
    }

}

