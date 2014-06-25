import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ByteConverterTest {
	private VertexInfo vertexValue;
	private EdgeValue edgeValue;

	@Before
	public void setUp() throws Exception {
		// vertexValue
		vertexValue = new VertexInfo(100, true);
		vertexValue.minB = 100;
		vertexValue.minF = 1000;

		// edgeValue
		edgeValue = new EdgeValue();
		edgeValue.minValue = 100;
	}

	@Test
	public void testEdgeValueConverter() {
		EdgeValueConverter converter = new EdgeValueConverter();
		byte[] array = new byte[converter.sizeOf()];
		converter.setValue(array, edgeValue);
		EdgeValue newValue = converter.getValue(array);
		
		assertEquals(edgeValue.minValue, newValue.minValue);

	}

	@Test
	public void testSCCInfoConverter() {
		VertexInfoConverter converter = new VertexInfoConverter();
		byte[] array = new byte[converter.sizeOf()];
		converter.setValue(array, vertexValue);
		VertexInfo newValue = converter.getValue(array);
		
		assertEquals(vertexValue.color, newValue.color);
		assertEquals(vertexValue.confirmed, newValue.confirmed);
		assertEquals(vertexValue.minB, newValue.minB);
		assertEquals(vertexValue.minF, newValue.minF);
	}

}
