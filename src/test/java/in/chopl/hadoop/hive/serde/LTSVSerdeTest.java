package in.chopl.hadoop.hive.serde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.beans.editors.DoubleEditor;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class LTSVSerdeTest {
    private LTSVSerde instance = new LTSVSerde();
    private Properties tbl = new Properties();
    private Configuration conf = null;

    @Before
    public void setUp() throws Exception {
        String cols = "boolean,tinyint,smallint,int,bigint,float,double,string,decimal";
        tbl.setProperty(serdeConstants.LIST_COLUMNS, cols);
        tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, cols);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testInitialize() throws Exception {
    }

    @Test
    public void testGetSerializedClass() throws Exception {

    }

    @Test
    public void testSerialize() throws Exception {

    }

    @Test
    public void testDeserialize() throws Exception {
        instance.initialize(conf, tbl);

        Writable w = new Text("boolean:true\ttinyint:1\tsmallint:1\tint:1\tbigint:1\tfloat:1.0\tdouble:1.0\tstring:foo\tdecimal:1");

        List<Object> result = (List<Object>) instance.deserialize(w);

        assertEquals(result.size(), 9);

        Object b = result.get(0);
        Object ti = result.get(1);
        Object si = result.get(2);
        Object i = result.get(3);
        Object bi = result.get(4);
        Object f = result.get(5);
        Object d = result.get(6);
        Object s = result.get(7);
        Object decimal = result.get(8);

        assertTrue(b instanceof Boolean);
        assertEquals((Boolean) b, true);

        assertTrue(ti instanceof Byte);
        assertEquals((Byte) ti, Byte.valueOf((byte) 1));

        assertTrue(si instanceof Short);
        assertEquals((Short) si, Short.valueOf((short) 1));

        assertTrue(i instanceof Integer);
        assertEquals((Integer) i, Integer.valueOf(1));

        assertTrue(bi instanceof Long);
        assertEquals((Long) bi, Long.valueOf((long) 1));

        assertTrue(f instanceof Float);
        assertEquals((Float) f, Float.valueOf((float) 1.0));

        assertTrue(d instanceof Double);
        assertEquals((Double) d, Double.valueOf(1.0));

        assertTrue(s instanceof String);
        assertEquals((String) s, "foo");

        assertTrue(decimal instanceof HiveDecimal);
        assertEquals((HiveDecimal) decimal, new HiveDecimal(1));
    }

    @Test
    public void testGetObjectInspector() throws Exception {

    }

    @Test
    public void testGetSerDeStats() throws Exception {

    }
}
