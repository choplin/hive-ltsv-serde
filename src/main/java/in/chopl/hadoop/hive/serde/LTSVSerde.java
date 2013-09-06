package in.chopl.hadoop.hive.serde;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class LTSVSerde implements SerDe {
    public static final Log log = LogFactory.getLog(LTSVSerde.class.getName());

    private final String LTSV_FIELD_SEPARATOR = "\t";
    private final String LTSV_KV_SEPARATOR = ":";

    // cached objectinspector for deserialized object
    private LTSVObjectInspector rowOI;

    // cached deserialized object
    private List<Object> deserialized;

    // cached serialized object
    private Text serialized;

    // caches for SerdeStats
    private long serializedSize;
    private SerDeStats stats;
    private boolean lastOperationSerialize;
    private boolean lastOperationDeserialize;

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        List<String> columnNames = Arrays.asList(columnNameProperty.split(","));

        List<TypeInfo> columnTypes = TypeInfoUtils
                .getTypeInfosFromTypeString(columnTypeProperty);

        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
        for (TypeInfo ti : columnTypes) {
            // support only primitive types
            if (ti.getCategory() != Category.PRIMITIVE) {
                throw new SerDeException(getClass().getName() + " doesn't allow column type " + ti);
            }

            ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(ti);
            columnOIs.add(oi);
        }
        if (log.isDebugEnabled()) {
            log.debug("column names: " + columnNames);
            log.debug("column types: " + columnTypes);
        }

        rowOI = new LTSVObjectInspector(columnNames, columnOIs);

        // initialize deserialized object
        int colNum = rowOI.getNumColumns();
        deserialized = new ArrayList<Object>(colNum);
        for (int i = 0; i < colNum; i++) {
            deserialized.add(null);
        }

        // initialize serialized object
        serialized = new Text();

        // initialize variables for SerdeStats
        serializedSize = 0;
        stats = new SerDeStats();
        lastOperationSerialize = false;
        lastOperationDeserialize = false;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        LTSVObjectInspector ltsvoi = (LTSVObjectInspector) objectInspector;
        List<? extends StructField> fields = ltsvoi.getAllStructFieldRefs();

        StringBuffer buf = new StringBuffer();

        for (StructField f : fields) {
            buf.append(f.getFieldName());
            buf.append(":");

            Object v = ltsvoi.getStructFieldData(o, f);
            PrimitiveObjectInspector poi =  (PrimitiveObjectInspector) f.getFieldObjectInspector();

            switch (poi.getPrimitiveCategory()) {
                case VOID:
                    break;
                case BOOLEAN:
                    buf.append(((BooleanObjectInspector) poi).get(v));
                    break;
                case BYTE:
                    buf.append(((ByteObjectInspector) poi).get(v));
                    break;
                case SHORT:
                    buf.append(((ShortObjectInspector) poi).get(v));
                    break;
                case INT:
                    buf.append(((IntObjectInspector) poi).get(v));
                    break;
                case LONG:
                    buf.append(((LongObjectInspector) poi).get(v));
                    break;
                case FLOAT:
                    buf.append(((FloatObjectInspector) poi).get(v));
                    break;
                case DOUBLE:
                    buf.append(((DoubleObjectInspector) poi).get(v));
                    break;
                case TIMESTAMP:
                    buf.append(((DoubleObjectInspector) poi).get(v));
                    break;
                case BINARY:
                    buf.append(((DoubleObjectInspector) poi).get(v));
                    break;
                case STRING:
                    buf.append(((StringObjectInspector) poi).getPrimitiveJavaObject(v));
                    break;
                case DECIMAL:
                    buf.append(((HiveDecimalObjectInspector) poi).getPrimitiveJavaObject(v).toString());
                    break;
                default:
                    break;
            }

        }


        serialized.set(buf.toString());

        return serialized;
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        // reset deserialized object
        for (Object obj : deserialized) {
            obj  = null;
        }

        Text rowText = (Text) blob;

        if (log.isDebugEnabled()) log.debug("row: " + rowText.toString());

        List<String> keyValues = Arrays.asList(rowText.toString().split(LTSV_FIELD_SEPARATOR));

        for (String keyValueString : keyValues) {
            if (! keyValueString.contains(LTSV_KV_SEPARATOR)) continue;

            String[] keyValue = keyValueString.split(LTSV_KV_SEPARATOR, 2);

            Integer index = rowOI.getFieldIndexWithName(keyValue[0]);

            if (index == null) continue;

            String v = keyValue[1];
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector) rowOI.getFieldObjectInspectorWithIndex(index);

            if (log.isDebugEnabled()) log.debug("key:" + keyValue[0] + " value:" + keyValue[1] + " category:" + poi.getPrimitiveCategory());

            switch (poi.getPrimitiveCategory()) {
                case VOID:
                    deserialized.set(index, null);
                    break;
                case BOOLEAN:
                    Boolean bool;
                    bool = Boolean.valueOf(v);
                    deserialized.set(index, bool);
                    break;
                case BYTE:
                    Byte b;
                    b = Byte.valueOf(v);
                    deserialized.set(index,b);
                    break;
                case SHORT:
                    Short s;
                    s = Short.valueOf(v);
                    deserialized.set(index,s);
                    break;
                case INT:
                    Integer i;
                    i = Integer.valueOf(v);
                    deserialized.set(index, i);
                    break;
                case LONG:
                    Long l;
                    l = Long.valueOf(v);
                    deserialized.set(index, l);
                    break;
                case FLOAT:
                    Float f;
                    f = Float.valueOf(v);
                    deserialized.set(index,f);
                    break;
                case DOUBLE:
                    Double d;
                    d = Double.valueOf(v);
                    deserialized.set(index,d);
                    break;
                //TODO: case PrimitiveCategory.TIMESTAMP:
                //TODO: case PrimitiveCategory.BINARY:
                case STRING:
                    deserialized.set(index, v);
                    break;
                case DECIMAL:
                    HiveDecimal bd;
                    bd = new HiveDecimal(v);
                    deserialized.set(index, bd);
                    break;
            }
        }
        return deserialized;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }

    @Override
    public SerDeStats getSerDeStats() {
        /*
        assert (lastOperationSerialize != lastOperationDeserialize);

        if (lastOperationSerialize) {
            stats.setRawDataSize(serializedSize);
        } else {
            stats.setRawDataSize(serializedSize);
        }
        return stats;
        */
        return null;
    }
}
