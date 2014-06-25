import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ByteConverterTest {
	private SCCInfo vertexValue;
	private BiDirLabel edgeValue;

	@Before
	public void setUp() throws Exception {
		// vertexValue
		vertexValue = new SCCInfo(100, true);

		// edgeValue
		edgeValue = new BiDirLabel();
		edgeValue.smallerOne = 100;
		edgeValue.largerOne = 1000;
	}

	@Test
	public void testBiDirLabelConverter() {
		BiDirLabelConverter converter = new BiDirLabelConverter();
		byte[] array = new byte[converter.sizeOf()];
		converter.setValue(array, edgeValue);
		BiDirLabel newValue = converter.getValue(array);
		
		assertEquals(edgeValue.smallerOne, newValue.smallerOne);
		assertEquals(edgeValue.largerOne, newValue.largerOne);

	}

	@Test
	public void testSCCInfoConverter() {
		SCCInfoConverter converter = new SCCInfoConverter();
		byte[] array = new byte[converter.sizeOf()];
		converter.setValue(array, vertexValue);
		SCCInfo newValue = converter.getValue(array);
		
		assertEquals(vertexValue.color, newValue.color);
		assertEquals(vertexValue.confirmed, newValue.confirmed);
	}

}
