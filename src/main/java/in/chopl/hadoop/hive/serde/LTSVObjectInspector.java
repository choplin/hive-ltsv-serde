package in.chopl.hadoop.hive.serde;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LTSVObjectInspector extends StandardStructObjectInspector{
    public static final Log log = LogFactory.getLog(LTSVObjectInspector.class.getName());

    private int numColumns;
    private Map<String, Integer> fieldIndexMap = new HashMap<String, Integer>();
    private List<ObjectInspector> types;

    protected LTSVObjectInspector(List<String> structFieldNames, List<ObjectInspector> structFieldObjectInspectors) {
        super(structFieldNames, structFieldObjectInspectors);

        assert structFieldNames.size() == structFieldObjectInspectors.size();

        numColumns = structFieldNames.size();

        for (int i = 0; i < numColumns; i++) {
            fieldIndexMap.put(structFieldNames.get(i), i);
        }

        types = structFieldObjectInspectors;
    }

    public Integer getFieldIndexWithName(String fieldName) {
        return fieldIndexMap.get(fieldName);
    }

    public ObjectInspector getFieldObjectInspectorWithIndex(int index) {
        return types.get(index);
    }

    public int getNumColumns() { return numColumns; }
}
