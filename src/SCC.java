import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Logger;

import org.apache.commons.lang.ArrayUtils;

import edu.cmu.graphchi.ChiEdge;
import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexProcessor;

/**
 * Example application for computing the weakly connected components of a graph.
 * The algorithm uses label exchange: each vertex first chooses a label equaling
 * its id; on the subsequent iterations each vertex sets its label to be the
 * minimum of the neighbors' labels and its current label. Algorithm finishes
 * when no labels change. Each vertex with same label belongs to same component.
 * 
 * @author akyrola
 */

public class SCC {

	private static Logger logger = ChiLogger.getLogger("SCC");

	// 프로그램에서 직접 사용
	public static int superstep = 0;
	public static int CONTRACTED_GRAPH_OUTPUT = 0;
	public static boolean firstIteration = true;
	public static boolean remainingVertices = true;

	/**
	 * Initialize the sharder-program.
	 * 
	 * @param graphName
	 * @param numShards
	 * @return
	 * @throws java.io.IOException
	 */
	protected static FastSharder createSharder(String graphName, int numShards)
			throws IOException {
		return new FastSharder<Integer, Integer>(graphName, numShards,
				new VertexProcessor<Integer>() {
					public Integer receiveVertexValue(int vertexId, String token) {
						return 0;
					}
				}, new EdgeProcessor<Integer>() {
					public Integer receiveEdge(int from, int to, String token) {
						return 0;
					}
				}, new IntConverter(), new IntConverter());
	}

	/**
	 * Usage: java edu.cmu.graphchi.demo.ConnectedComponents graph-name
	 * num-shards filetype(edgelist|adjlist) For specifying the number of
	 * shards, 20-50 million edges/shard is often a good configuration.
	 */
	public static void main(String[] args) throws Exception {
		String baseFilename = args[0];
		int nShards = Integer.parseInt(args[1]);
		String fileType = (args.length >= 3 ? args[2] : null);

		/* Create shards */
		FastSharder sharder = createSharder(baseFilename, nShards);
		if (baseFilename.equals("pipein")) { // Allow piping graph in
			sharder.shard(System.in, fileType);
		} else {
			if (!new File(ChiFilenames.getFilenameIntervals(baseFilename,
					nShards)).exists()) {
				sharder.shard(new FileInputStream(new File(baseFilename)),
						fileType);
			} else {
				logger.info("Found shards -- no need to preprocess");
			}
		}

		/* Run GraphChi ... */
		GraphChiEngine<SCCInfo, BiDirLabel> engine = new GraphChiEngine<SCCInfo, BiDirLabel>(
				baseFilename, nShards);
		engine.setVertexDataConverter(new SCCInfoConverter());
		engine.setEdataConverter(new BiDirLabelConverter());
		engine.setEnableScheduler(true);
		engine.run(new SCCForward(), 1000);

	}

}

class SCCForward implements GraphChiProgram<SCCInfo, BiDirLabel> {

	@Override
	public void update(ChiVertex<SCCInfo, BiDirLabel> vertex,
			GraphChiContext context) {
		if (SCC.firstIteration) {
			vertex.setValue(new SCCInfo(vertex.getId()));
		}

		if (vertex.getValue().confirmed) {
			VertexUtil.removeAllEdges(vertex);

			return;
		}

		if (vertex.numInEdges() == 0 || vertex.numOutEdges() == 0) {
			if (vertex.numEdges() > 0) {
				vertex.setValue(new SCCInfo(vertex.getId(), true));
			}

			VertexUtil.removeAllEdges(vertex);
			return;
		}

		SCC.remainingVertices = true;

		SCCInfo vertexData = vertex.getValue();
		boolean propagate = false;
		if (context.getIteration() == 0) {
			/*
			 * TODO: 검증 필요 [원본] - vertecData = vertex.getId()
			 */
			vertexData = new SCCInfo(vertex.getId());
			propagate = true;

			// Clean up in-edges. This would be nicer in the messaging
			// abstraction...
			for (int i = 0; i < vertex.numInEdges(); i++) {
				BiDirLabel edgeData = vertex.inEdge(i).getValue();
				if (!edgeData.deleted()) {
					edgeData.setMyLabel(vertex.getId(), vertex.inEdge(i)
							.getVertexId(), vertex.getId());
				}
			}
		} else {
			int minid = vertexData.color;
			for (int i = 0; i < vertex.numInEdges(); i++) {
				if (!vertex.inEdge(i).getValue().deleted()) {
					minid = Math.min(
							minid,
							vertex.inEdge(i)
									.getValue()
									.getNeighborLabel(vertex.getId(),
											vertex.inEdge(i).getVertexId()));
				}
			}

			if (minid != vertexData.color) {
				vertexData.color = minid;
				propagate = true;
			}
		}
	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
		// TODO Auto-generated method stub

	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		SCC.firstIteration = false;

	}

	@Override
	public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
		// TODO Auto-generated method stub

	}

	@Override
	public void endInterval(GraphChiContext ctx, VertexInterval interval) {
		// TODO Auto-generated method stub

	}

	@Override
	public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
		// TODO Auto-generated method stub

	}

	@Override
	public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
		// TODO Auto-generated method stub

	}

}

class BiDirLabel implements Serializable {
	private static final long serialVersionUID = -6638107748426892170L;
	public static final int DELETED = -1;
	public int smallerOne;
	public int largerOne;

	public void setNeighborLabel(int myid, int nbid, int newValue) {
		if (myid < nbid) {
			largerOne = newValue;
		} else {
			smallerOne = newValue;
		}
	}

	public Integer getNeighborLabel(int myid, int nbid) {
		if (myid < nbid) {
			return largerOne;
		} else {
			return smallerOne;
		}
	}

	public void setMyLabel(int myid, int nbid, int newValue) {
		if (myid < nbid) {
			smallerOne = newValue;
		} else {
			largerOne = newValue;
		}
	}

	public Integer getMyLabel(int myid, int nbid) {
		if (myid < nbid) {
			return smallerOne;
		} else {
			return largerOne;
		}
	}

	public boolean deleted() {
		return (smallerOne == DELETED);
	}

	@Override
	public String toString() {
		return String.format("%d, %d", smallerOne, largerOne);
	}
}

class BiDirLabelConverter implements BytesToValueConverter<BiDirLabel> {

	@Override
	public int sizeOf() {
		return 8;
	}

	@Override
	public BiDirLabel getValue(byte[] array) {
		BiDirLabel val = new BiDirLabel();
		val.smallerOne = ((array[3] & 0xff) << 24) + ((array[2] & 0xff) << 16)
				+ ((array[1] & 0xff) << 8) + (array[0] & 0xff);
		val.largerOne = ((array[7] & 0xff) << 24) + ((array[6] & 0xff) << 16)
				+ ((array[5] & 0xff) << 8) + (array[4] & 0xff);

		return val;
	}

	@Override
	public void setValue(byte[] array, BiDirLabel val) {
		IntConverter intConverter = new IntConverter();

		byte[] smaller = new byte[4];
		intConverter.setValue(smaller, val.smallerOne);

		byte[] larger = new byte[4];
		intConverter.setValue(larger, val.largerOne);

		array = ArrayUtils.addAll(smaller, larger);

	}

}

class SCCInfo {
	public int color;
	public boolean confirmed;

	public int getColor() {
		return color;
	}

	public void setColor(int color) {
		this.color = color;
	}

	public boolean isConfirmed() {
		return confirmed;
	}

	public void setConfirmed(boolean confirmed) {
		this.confirmed = confirmed;
	}

	public SCCInfo() {
		this.color = 0;
		this.confirmed = false;
	}

	public SCCInfo(int color) {
		this.color = color;
		this.confirmed = false;
	}

	public SCCInfo(int color, boolean confirmed) {
		this.color = color;
		this.confirmed = confirmed;
	}
}

class SCCInfoConverter implements BytesToValueConverter<SCCInfo> {

	@Override
	public int sizeOf() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public SCCInfo getValue(byte[] array) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setValue(byte[] array, SCCInfo val) {
		// TODO Auto-generated method stub

	}

}

class VertexUtil {
	public static void removeAllEdges(ChiVertex<SCCInfo, BiDirLabel> vertex) {
		if (vertex.numEdges() > 0) {
			// remove all edges of the vertex
			for (int i = 0; i < vertex.numInEdges(); i++) {
				ChiEdge<BiDirLabel> e = vertex.inEdge(i);
				e.getValue().largerOne = BiDirLabel.DELETED;
				e.getValue().smallerOne = BiDirLabel.DELETED;
			}

			for (int i = 0; i < vertex.numOutEdges(); i++) {
				ChiEdge<BiDirLabel> e = vertex.outEdge(i);
				e.getValue().largerOne = BiDirLabel.DELETED;
				e.getValue().smallerOne = BiDirLabel.DELETED;
			}
		}
	}
}