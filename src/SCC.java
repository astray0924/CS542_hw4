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
import edu.cmu.graphchi.util.LabelAnalysis;

public class SCC {

	private static Logger logger = ChiLogger.getLogger("SCC");

	// 프로그램에서 직접 사용
	public static int superstep = 0;
	public static boolean firstIteration = true;
	public static boolean remainingVertices = true;

	// public static GraphChiEngine<SCCInfo, BiDirLabel> engine = null;

	/**
	 * Initialize the sharder-program.
	 * 
	 * @param graphName
	 * @param numShards
	 * @return
	 * @throws java.io.IOException
	 */
	protected static FastSharder<VertexInfo, EdgeValue> createSharder(
			String graphName, int numShards) throws IOException {
		return new FastSharder<VertexInfo, EdgeValue>(graphName, numShards,
				new VertexProcessor<VertexInfo>() {
					public VertexInfo receiveVertexValue(int vertexId,
							String token) {
						return new VertexInfo();
					}
				}, new EdgeProcessor<EdgeValue>() {
					public EdgeValue receiveEdge(int from, int to, String token) {
						return new EdgeValue();
					}
				}, new VertexInfoConverter(), new EdgeValueConverter());
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
		GraphChiEngine<VertexInfo, EdgeValue> engine = null;
		while (SCC.remainingVertices) {
			System.out.println("Starting Superstep: " + SCC.superstep + "\n");
			SCC.superstep++;
			SCC.remainingVertices = false;

			SCCForward forward = new SCCForward();
			engine = new GraphChiEngine<VertexInfo, EdgeValue>(baseFilename,
					nShards);
			engine.setVertexDataConverter(new VertexInfoConverter());
			engine.setEdataConverter(new EdgeValueConverter());
			engine.setEnableScheduler(true);
			engine.run(forward, 1000);

			// if (SCC.remainingVertices) {
			// System.out.println("Starting Backward \n");
			//
			// SCCBackward backward = new SCCBackward();
			// GraphChiEngine<SCCInfo, BiDirLabel> engine2 = new
			// GraphChiEngine<SCCInfo, BiDirLabel>(
			// baseFilename, nShards);
			// engine2.setVertexDataConverter(new SCCInfoConverter());
			// engine2.setEdataConverter(new BiDirLabelConverter());
			// engine2.setEnableScheduler(true);
			// // engine.setSaveEdgefilesAfterInmemmode(true);
			// engine2.run(backward, 1);
			//
			// int origNumShards = engine2.getIntervals().size();
			//
			// if (origNumShards > 1) {
			// // TODO: Contract deleted edges
			// }
			// }
		}

		// logger.info("Ready. Going to output...");
		//
		// /* Process output. The output file has format <vertex-id,
		// component-id> */
		// LabelAnalysis.computeLabels(baseFilename, engine.numVertices(),
		// engine.getVertexIdTranslate());
		//
		// logger.info("Finished. See file: " + baseFilename + ".components");

		/*
		 * [참고] Translating between the internal ids and original ids is easy
		 * using the VertexIdTranslate class
		 */
		// VertexIdTranslate trans = engine.getVertexIdTranslate();
		// for(int i=0; i < engine.numVertices(); i++) {
		// System.out.println("Internal id " + i + " = original id " +
		// trans.backward(i));
		// }
	}

}

class SCCForward implements GraphChiProgram<VertexInfo, EdgeValue> {

	@Override
	public void update(ChiVertex<VertexInfo, EdgeValue> vertex,
			GraphChiContext context) {
		if (SCC.firstIteration) {
			vertex.setValue(new VertexInfo(vertex.getId()));
		}

		if (vertex.getValue().confirmed) {
			VertexUtil.removeAllEdges(vertex);
			return;
		}

		if (vertex.numInEdges() == 0 || vertex.numOutEdges() == 0) {
			if (vertex.numEdges() > 0) {
				vertex.setValue(new VertexInfo(vertex.getId(), true));
			}
			VertexUtil.removeAllEdges(vertex);
			return;
		}

	}

	@Override
	public void beginIteration(GraphChiContext ctx) {

	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		SCC.firstIteration = false;

	}

	@Override
	public void beginInterval(GraphChiContext ctx, VertexInterval interval) {

	}

	@Override
	public void endInterval(GraphChiContext ctx, VertexInterval interval) {

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

class SCCBackward implements GraphChiProgram<VertexInfo, EdgeValue> {

	@Override
	public void update(ChiVertex<VertexInfo, EdgeValue> vertex,
			GraphChiContext context) {
		// if (vertex.getValue().confirmed) {
		// return;
		// }
		//
		// SCCInfo vertexData = vertex.getValue();
		// boolean propagate = false;
		//
		// if (context.getIteration() == 0) {
		// // "Leader" of the SCC
		// if (vertexData.color == vertex.getId()) {
		// propagate = true;
		// VertexUtil.removeAllOutEdges(vertex);
		// }
		// } else {
		// // Loop over in-edges and see if there is a match
		// boolean match = false;
		// for (int i = 0; i < vertex.numOutEdges(); i++) {
		// if (!vertex.outEdge(i).getValue().deleted()) {
		// if (vertex
		// .outEdge(i)
		// .getValue()
		// .getNeighborLabel(vertex.getId(),
		// vertex.outEdge(i).getVertexId()) == vertexData.color) {
		// match = true;
		//
		// break;
		// }
		// }
		// }
		//
		// if (match) {
		// propagate = true;
		// VertexUtil.removeAllOutEdges(vertex);
		// vertex.setValue(new SCCInfo(vertexData.color, true));
		// } else {
		// vertex.setValue(new SCCInfo(vertex.getId(), false));
		// }
		// }
		//
		// if (propagate) {
		// for (int i = 0; i < vertex.numInEdges(); i++) {
		// BiDirLabel edgeData = vertex.inEdge(i).getValue();
		// if (!edgeData.deleted()) {
		// edgeData.setMyLabel(vertex.getId(), vertex.inEdge(i)
		// .getVertexId(), vertexData.color);
		// vertex.inEdge(i).setValue(edgeData);
		// context.getScheduler().addTask(
		// vertex.inEdge(i).getVertexId());
		// }
		// }
		// }

	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
		// TODO Auto-generated method stub

	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		// TODO Auto-generated method stub

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

class EdgeValue implements Serializable {
	private static final long serialVersionUID = -6638107748426892170L;
	public static final int DELETED = -1;
	public int minValue;

	public void updateMinValue(int value) {
		if (value < minValue) {
			minValue = value;
		}
	}

	public int getMinValue() {
		return minValue;
	}

	public boolean deleted() {
		return (minValue == DELETED);
	}

	@Override
	public String toString() {
		return String.format("%d", minValue);
	}
}

class EdgeValueConverter implements BytesToValueConverter<EdgeValue> {

	@Override
	public int sizeOf() {
		return 4;
	}

	@Override
	public EdgeValue getValue(byte[] array) {
		IntConverter intConverter = new IntConverter();

		EdgeValue val = new EdgeValue();
		val.minValue = intConverter.getValue(array);

		return val;
	}

	@Override
	public void setValue(byte[] array, EdgeValue val) {
		IntConverter intConverter = new IntConverter();
		intConverter.setValue(array, val.minValue);
	}

}

class VertexInfo {
	public int color;
	public boolean confirmed;
	public int minF;
	public int minB;

	public VertexInfo() {
		this.color = 0;
		this.confirmed = false;
		this.minF = Integer.MAX_VALUE;
		this.minB = Integer.MAX_VALUE;
	}

	public VertexInfo(int color) {
		this();
		this.color = color;
	}

	public VertexInfo(int color, boolean confirmed) {
		this();
		this.color = color;
		this.confirmed = confirmed;
	}

	@Override
	public String toString() {
		return String.format("%s, %s, %s, %s", color, minF, minB, confirmed);
	}
}

class VertexInfoConverter implements BytesToValueConverter<VertexInfo> {

	@Override
	public int sizeOf() {
		return 16;
	}

	@Override
	public VertexInfo getValue(byte[] array) {
		IntConverter intConverter = new IntConverter();

		byte[] colorByte = ArrayUtils.subarray(array, 0, 4);
		int color = intConverter.getValue(colorByte);

		byte[] confirmedByte = ArrayUtils.subarray(array, 4, 8);
		boolean confirmed = (intConverter.getValue(confirmedByte) == 0) ? false
				: true;

		byte[] minFByte = ArrayUtils.subarray(array, 8, 12);
		int minF = intConverter.getValue(minFByte);

		byte[] minBByte = ArrayUtils.subarray(array, 12, 16);
		int minB = intConverter.getValue(minBByte);

		VertexInfo info = new VertexInfo(color, confirmed);
		info.minF = minF;
		info.minB = minB;

		return info;
	}

	@Override
	public void setValue(byte[] array, VertexInfo val) {
		IntConverter intConverter = new IntConverter();

		byte[] colorByte = new byte[4];
		intConverter.setValue(colorByte, val.color);

		byte[] confirmedByte = new byte[4];
		intConverter.setValue(confirmedByte, val.confirmed ? 1 : 0);

		byte[] minFByte = new byte[4];
		intConverter.setValue(minFByte, val.minF);

		byte[] minBByte = new byte[4];
		intConverter.setValue(minBByte, val.minB);

		array[0] = colorByte[0];
		array[1] = colorByte[1];
		array[2] = colorByte[2];
		array[3] = colorByte[3];

		array[4] = confirmedByte[0];
		array[5] = confirmedByte[1];
		array[6] = confirmedByte[2];
		array[7] = confirmedByte[3];

		array[8] = minFByte[0];
		array[9] = minFByte[1];
		array[10] = minFByte[2];
		array[11] = minFByte[3];

		array[12] = minBByte[0];
		array[13] = minBByte[1];
		array[14] = minBByte[2];
		array[15] = minBByte[3];
	}

}

class VertexUtil {
	public static void removeAllEdges(ChiVertex<VertexInfo, EdgeValue> vertex) {
		// remove all edges of the vertex
		removeAllInEdges(vertex);
		removeAllOutEdges(vertex);
	}

	public static void removeAllInEdges(ChiVertex<VertexInfo, EdgeValue> vertex) {
		for (int i = 0; i < vertex.numInEdges(); i++) {
			ChiEdge<EdgeValue> e = vertex.inEdge(i);
			EdgeValue val = e.getValue();
			val.minValue = EdgeValue.DELETED;
			e.setValue(val);
		}
	}

	public static void removeAllOutEdges(ChiVertex<VertexInfo, EdgeValue> vertex) {
		for (int i = 0; i < vertex.numOutEdges(); i++) {
			ChiEdge<EdgeValue> e = vertex.outEdge(i);
			EdgeValue val = e.getValue();
			val.minValue = EdgeValue.DELETED;
			e.setValue(val);
		}
	}
}