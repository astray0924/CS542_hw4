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
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.preprocessing.VertexProcessor;

public class SCC {

	private static Logger logger = ChiLogger.getLogger("SCC");

	// 프로그램에서 직접 사용
	public static int superstep = 0;
	public static boolean firstIteration = true;
	public static boolean remainingVertices = true;

	public static GraphChiEngine<VertexValue, EdgeValue> engine = null;

	/**
	 * Initialize the sharder-program.
	 * 
	 * @param graphName
	 * @param numShards
	 * @return
	 * @throws java.io.IOException
	 */
	protected static FastSharder<VertexValue, EdgeValue> createSharder(
			String graphName, int numShards) throws IOException {
		return new FastSharder<VertexValue, EdgeValue>(graphName, numShards,
				new VertexProcessor<VertexValue>() {
					public VertexValue receiveVertexValue(int vertexId,
							String token) {
						return new VertexValue();
					}
				}, new EdgeProcessor<EdgeValue>() {
					public EdgeValue receiveEdge(int from, int to, String token) {
						EdgeValue edgeValue = new EdgeValue();
						edgeValue.from = from;
						edgeValue.to = to;

						return edgeValue;
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

		// Forward
		SCCForward forward = new SCCForward();
		engine = new GraphChiEngine<VertexValue, EdgeValue>(baseFilename,
				nShards);
		engine.setVertexDataConverter(new VertexInfoConverter());
		engine.setEdataConverter(new EdgeValueConverter());
		engine.setEnableScheduler(true);
		engine.run(forward, 1000);

		// Backward
		engine.run(new SCCBackward(), 1000);

		// Debug
		engine.run(new GraphDebug(), 1);
	}

}

class GraphDebug implements GraphChiProgram<VertexValue, EdgeValue> {

	@Override
	public void update(ChiVertex<VertexValue, EdgeValue> vertex,
			GraphChiContext context) {
		VertexIdTranslate translator = SCC.engine.getVertexIdTranslate();

		System.out.println(String.format(
				"%s - MinF: %s / MinB: %s / Color: %s", vertex.getId(), vertex
						.getValue().getMinF(), vertex.getValue().getMinB(),
				vertex.getValue().color));

		// for (int i = 0; i < vertex.numInEdges(); i++) {
		// EdgeValue e = vertex.inEdge(i).getValue();
		//
		// System.out.println(String.format("%s => %s", e.from, e.to));
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

class SCCForward implements GraphChiProgram<VertexValue, EdgeValue> {

	@Override
	public void update(ChiVertex<VertexValue, EdgeValue> vertex,
			GraphChiContext context) {
		if (SCC.firstIteration) {
			VertexValue vertexValue = new VertexValue();
			vertexValue.updateMinF(vertex.getId());
			vertex.setValue(vertexValue);
		}

		if (vertex.getValue().confirmed) {
			VertexUtil.removeAllEdges(vertex);
			return;
		}

		if (vertex.numInEdges() == 0 || vertex.numOutEdges() == 0) {
			if (vertex.numEdges() > 0) {
				VertexValue value = new VertexValue();
				value.confirmed = true;
				value.updateMinF(vertex.getId());
				VertexUtil.removeAllEdges(vertex);
			}

			// System.out.println(String.format("Dangling: %s",
			// vertex.getId()));

			return;
		}

		SCC.remainingVertices = true;

		VertexValue vertexData = vertex.getValue();
		boolean propagate = false;

		if (context.getIteration() == 0) {
			// vertexData = new VertexValue();
			// vertexData.updateMinF(vertex.getId());
			propagate = true;
		} else {
			int minid = vertexData.getMinF();
			for (int i = 0; i < vertex.numInEdges(); i++) {
				if (!vertex.inEdge(i).getValue().deleted()) {
					minid = Math.min(minid, vertex.inEdge(i).getValue()
							.getMinF());
				}
			}

			if (minid != vertexData.getMinF()) {
				vertexData.updateMinF(minid);
				propagate = true;
			}
		}
		vertex.setValue(vertexData);

		if (propagate) {
			for (int i = 0; i < vertex.numOutEdges(); i++) {
				EdgeValue edgeData = vertex.outEdge(i).getValue();

				if (!edgeData.deleted()) {
					edgeData.updateMinF(vertexData.getMinF());
					vertex.outEdge(i).setValue(edgeData);
					context.getScheduler().addTask(
							vertex.outEdge(i).getVertexId());
				}
			}
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

class SCCBackward implements GraphChiProgram<VertexValue, EdgeValue> {

	@Override
	public void update(ChiVertex<VertexValue, EdgeValue> vertex,
			GraphChiContext context) {
		if (vertex.getValue().confirmed) {
			return;
		}

		VertexValue vertexData = vertex.getValue();
		boolean propagate = false;

//		if (context.getIteration() == 0) {
//			// Backward 하기 전에 미리 간선 정보를 초기화
//			VertexUtil.resetAllEdges(vertex);
//
//			// "Leader" of the SCC
//			if (vertexData.getMinF() == vertex.getId()) {
//				vertexData.setMinB(vertex.getId());
//				vertexData.color = vertex.getId();
//				vertex.setValue(vertexData);
//				VertexUtil.removeAllOutEdges(vertex);
//				propagate = true;
//				
////				System.out.println("Leader: " + vertex.getId());
//			}
//			
//		} else {
//			boolean match = false;
//			
//			for (int i = 0; i < vertex.numOutEdges(); i++) {
//				int minval = vertexData.getMinB();
//				
//				if (!vertex.outEdge(i).getValue().deleted()) {
//					int bVal = vertex.outEdge(i).getValue().getMinB();
//					
//					minval = Math.min(minval, bVal);
//					
//					vertexData.updateMinB(minval);
//					vertex.setValue(vertexData);
//
//					if (vertex.getValue().getMinF() == vertex.getValue()
//							.getMinB()) {
//						match = true;
//						break;
//					}
//				}
//			}
//
//			if (match) {
//				propagate = true;
//				VertexUtil.removeAllOutEdges(vertex);
//
//				VertexValue val = vertex.getValue();
//				val.color = val.getMinB();
//				val.confirmed = true;
//				vertex.setValue(val);
//			} else {
//				VertexValue val = vertex.getValue();
//				val.color = vertex.getId();
//				val.confirmed = false;
//				vertex.setValue(val);
//			}
//		}
//
//		if (propagate) {
//			for (int i = 0; i < vertex.numInEdges(); i++) {
//				EdgeValue edgeData = vertex.inEdge(i).getValue();
//				if (!edgeData.deleted()) {
//					edgeData.updateMinB(vertex.getValue().getMinB());
//					vertex.inEdge(i).setValue(edgeData);
//
//					context.getScheduler().addTask(
//							vertex.inEdge(i).getVertexId());
//				}
//			}
//		}

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
	public int minF = Integer.MAX_VALUE;
	public int minB = Integer.MAX_VALUE;
	public int from = -1;
	public int to = -1;

	public void updateMinF(int value) {
		if (value < minF) {
			minF = value;
		}
	}

	public int getMinF() {
		return minF;
	}

	public void updateMinB(int value) {
		if (value < minB) {
			minB = value;
		}
	}

	public int getMinB() {
		return minB;
	}

	public boolean deleted() {
		return (minF == DELETED);
	}

	@Override
	public String toString() {
		return String.format("%d", minF);
	}
}

class EdgeValueConverter implements BytesToValueConverter<EdgeValue> {

	@Override
	public int sizeOf() {
		return 16;
	}

	@Override
	public EdgeValue getValue(byte[] array) {
		IntConverter intConverter = new IntConverter();

		EdgeValue val = new EdgeValue();
		val.minF = intConverter.getValue(ArrayUtils.subarray(array, 0, 4));
		val.minB = intConverter.getValue(ArrayUtils.subarray(array, 4, 8));
		val.from = intConverter.getValue(ArrayUtils.subarray(array, 8, 12));
		val.to = intConverter.getValue(ArrayUtils.subarray(array, 12, 16));

		return val;
	}

	@Override
	public void setValue(byte[] array, EdgeValue val) {
		IntConverter intConverter = new IntConverter();
		intConverter.setValue(array, val.minF);

		byte[] minByte = new byte[4];
		intConverter.setValue(minByte, val.minB);
		array[4] = minByte[0];
		array[5] = minByte[1];
		array[6] = minByte[2];
		array[7] = minByte[3];

		byte[] fromByte = new byte[4];
		intConverter.setValue(fromByte, val.from);
		array[8] = fromByte[0];
		array[9] = fromByte[1];
		array[10] = fromByte[2];
		array[11] = fromByte[3];

		byte[] toByte = new byte[4];
		intConverter.setValue(toByte, val.to);
		array[12] = toByte[0];
		array[13] = toByte[1];
		array[14] = toByte[2];
		array[15] = toByte[3];
	}

}

class VertexValue {
	public int color;
	public boolean confirmed;
	private int minF;
	private int minB;

	public VertexValue() {
		this.color = 0;
		this.confirmed = false;
		this.minF = Integer.MAX_VALUE;
		this.minB = Integer.MAX_VALUE;
	}

	public VertexValue(int color) {
		this();
		this.color = color;
	}

	public VertexValue(int color, boolean confirmed) {
		this();
		this.color = color;
		this.confirmed = confirmed;
	}

	public int getMinF() {
		return minF;
	}

	public void setMinF(int value) {
		minF = value;
	}

	public void updateMinF(int value) {
		if (value < minF) {
			minF = value;
		}
	}

	public int getMinB() {
		return minB;
	}

	public void setMinB(int value) {
		minB = value;
	}

	public void updateMinB(int value) {
		if (value < minB) {
			minB = value;
		}
	}

	@Override
	public String toString() {
		return String.format("%s, %s, %s, %s", color, minF, minB, confirmed);
	}
}

class VertexInfoConverter implements BytesToValueConverter<VertexValue> {

	@Override
	public int sizeOf() {
		return 16;
	}

	@Override
	public VertexValue getValue(byte[] array) {
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

		VertexValue info = new VertexValue(color, confirmed);
		info.setMinF(minF);
		info.setMinB(minB);

		return info;
	}

	@Override
	public void setValue(byte[] array, VertexValue val) {
		IntConverter intConverter = new IntConverter();

		byte[] colorByte = new byte[4];
		intConverter.setValue(colorByte, val.color);

		byte[] confirmedByte = new byte[4];
		intConverter.setValue(confirmedByte, val.confirmed ? 1 : 0);

		byte[] minFByte = new byte[4];
		intConverter.setValue(minFByte, val.getMinF());

		byte[] minBByte = new byte[4];
		intConverter.setValue(minBByte, val.getMinB());

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
	public static void resetAllEdges(ChiVertex<VertexValue, EdgeValue> vertex) {

		resetAllInEdges(vertex);
		resetAllOutEdges(vertex);

	}

	public static void resetAllInEdges(ChiVertex<VertexValue, EdgeValue> vertex) {
		for (int i = 0; i < vertex.numInEdges(); i++) {
			ChiEdge<EdgeValue> e = vertex.inEdge(i);
			EdgeValue val = new EdgeValue();
			val.from = e.getVertexId();
			val.to = vertex.getId();
			e.setValue(val);
		}
	}

	public static void resetAllOutEdges(ChiVertex<VertexValue, EdgeValue> vertex) {
		for (int i = 0; i < vertex.numOutEdges(); i++) {
			ChiEdge<EdgeValue> e = vertex.outEdge(i);
			EdgeValue val = new EdgeValue();
			val.from = vertex.getId();
			val.to = e.getVertexId();
			e.setValue(val);
		}
	}

	public static void removeAllEdges(ChiVertex<VertexValue, EdgeValue> vertex) {
		// remove all edges of the vertex
		removeAllInEdges(vertex);
		removeAllOutEdges(vertex);
	}

	public static void removeAllInEdges(ChiVertex<VertexValue, EdgeValue> vertex) {
		for (int i = 0; i < vertex.numInEdges(); i++) {
			ChiEdge<EdgeValue> e = vertex.inEdge(i);
			EdgeValue val = e.getValue();
			val.minF = EdgeValue.DELETED;
			e.setValue(val);
		}
	}

	public static void removeAllOutEdges(
			ChiVertex<VertexValue, EdgeValue> vertex) {
		for (int i = 0; i < vertex.numOutEdges(); i++) {
			ChiEdge<EdgeValue> e = vertex.outEdge(i);
			EdgeValue val = e.getValue();
			val.minF = EdgeValue.DELETED;
			e.setValue(val);
		}
	}
}