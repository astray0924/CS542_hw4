import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ByteConverterTest {
	private VertexValue vertexValue;
	private EdgeValue edgeValue;

	@Before
	public void setUp() throws Exception {
		// vertexValue
		vertexValue = new VertexValue(100, true);
		vertexValue.setMinB(100);
		vertexValue.setMinF(1000);
		vertexValue.id = 201;

		// edgeValue
		edgeValue = new EdgeValue();
		edgeValue.minValue = 100;
		edgeValue.from = 1;
		edgeValue.to = 2;
	}

	@Test
	public void testEdgeValueConverter() {
		EdgeValueConverter converter = new EdgeValueConverter();
		byte[] array = new byte[converter.sizeOf()];
		converter.setValue(array, edgeValue);
		EdgeValue newValue = converter.getValue(array);

		assertEquals(edgeValue.minValue, newValue.minValue);
		assertEquals(edgeValue.from, newValue.from);
		assertEquals(edgeValue.to, newValue.to);

	}

	@Test
	public void testSCCInfoConverter() {
		VertexInfoConverter converter = new VertexInfoConverter();
		byte[] array = new byte[converter.sizeOf()];
		converter.setValue(array, vertexValue);
		VertexValue newValue = converter.getValue(array);

		assertEquals(vertexValue.color, newValue.color);
		assertEquals(vertexValue.confirmed, newValue.confirmed);
		assertEquals(vertexValue.getMinB(), newValue.getMinB());
		assertEquals(vertexValue.getMinF(), newValue.getMinF());
		assertEquals(vertexValue.id, newValue.id);
	}

}
