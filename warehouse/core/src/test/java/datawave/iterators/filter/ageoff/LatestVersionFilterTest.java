package datawave.iterators.filter.ageoff;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import datawave.iterators.filter.ageoff.LatestVersionFilter.Mode;
import datawave.iterators.filter.ageoff.LatestVersionFilter.VersionFilterConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static datawave.iterators.filter.ageoff.LatestVersionFilter.DATATYPE_LIST_OPTION_NAME;
import static datawave.iterators.filter.ageoff.LatestVersionFilter.DATATYPE_MODE_OPTION_NAME;
import static datawave.iterators.filter.ageoff.LatestVersionFilter.IS_INDEX_TABLE_OPTION_NAME;

public class LatestVersionFilterTest {

    // todo add the tests that have defined input and output data and run through teardowns
    private LatestVersionFilter filter;

    @Before
    public void createFilter() {
        filter = new LatestVersionFilter();
    }

    @Test
    public void equalModeIndexTableScanAccept() {
        filter.isIndexTable = true;
        filter.isScanScope = true;
        assertIndexAccept(true, 0L, 0L, Mode.EQUAL);
        assertIndexAccept(false, 0L, 1L, Mode.EQUAL);
        assertIndexAccept(false, 1L, 0L, Mode.EQUAL);
    }

    @Test
    public void equalModeIndexTableNonScanAccept() {
        filter.isIndexTable = true;
        filter.isScanScope = false;
        assertIndexAccept(true, 0L, 0L, Mode.EQUAL);
        assertIndexAccept(false, 0L, 1L, Mode.EQUAL);
        assertIndexAccept(false, 1L, 0L, Mode.EQUAL);
    }

    @Test
    public void equalModeNonIndexTableScanAccept() {
        filter.isIndexTable = false;
        filter.isScanScope = true;
        assertNonIndexAccept(true, 0L, 0L, Mode.EQUAL);
        assertNonIndexAccept(false, 0L, 1L, Mode.EQUAL);
        assertNonIndexAccept(false, 1L, 0L, Mode.EQUAL);
    }

    @Test
    public void equalModeNonIndexTableNonScanAccept() {
        filter.isIndexTable = false;
        filter.isScanScope = false;
        assertNonIndexAccept(true, 0L, 0L, Mode.EQUAL);
        assertNonIndexAccept(false, 0L, 1L, Mode.EQUAL);
        assertNonIndexAccept(false, 1L, 0L, Mode.EQUAL);
    }

    @Test
    public void gteModeIndexTableScanAccept() {
        filter.isIndexTable = true;
        filter.isScanScope = true;
        assertIndexAccept(true, 0L, 0L, Mode.GREATER_THAN_OR_EQUAL);
        assertIndexAccept(false, 0L, 1L, Mode.GREATER_THAN_OR_EQUAL);
        assertIndexAccept(true, 1L, 0L, Mode.GREATER_THAN_OR_EQUAL);
    }

    @Test
    public void gteModeIndexTableNonScanAccept() {
        filter.isIndexTable = true;
        filter.isScanScope = false;
        assertIndexAccept(true, 0L, 0L, Mode.GREATER_THAN_OR_EQUAL);
        assertIndexAccept(false, 0L, 1L, Mode.GREATER_THAN_OR_EQUAL);
        assertIndexAccept(true, 1L, 0L, Mode.GREATER_THAN_OR_EQUAL);
    }

    @Test
    public void gteModeNonIndexTableScanAccept() {
        filter.isIndexTable = false;
        filter.isScanScope = true;
        assertNonIndexAccept(true, 0L, 0L, Mode.GREATER_THAN_OR_EQUAL);
        assertNonIndexAccept(false, 0L, 1L, Mode.GREATER_THAN_OR_EQUAL);
        assertNonIndexAccept(true, 1L, 0L, Mode.GREATER_THAN_OR_EQUAL);
    }

    @Test
    public void gteModeNonIndexTableNonScanAccept() {
        filter.isIndexTable = false;
        filter.isScanScope = false;
        assertNonIndexAccept(true, 0L, 0L, Mode.GREATER_THAN_OR_EQUAL);
        assertNonIndexAccept(false, 0L, 1L, Mode.GREATER_THAN_OR_EQUAL);
        assertNonIndexAccept(true, 1L, 0L, Mode.GREATER_THAN_OR_EQUAL);
    }

    @Test
    public void undefinedModeIndexTableScanAccept() {
        filter.isIndexTable = true;
        filter.isScanScope = true;
        assertIndexAccept(false, 0L, 0L, Mode.UNDEFINED);
        assertIndexAccept(false, 0L, 1L, Mode.UNDEFINED);
        assertIndexAccept(false, 1L, 0L, Mode.UNDEFINED);
    }
    @Test
    public void undefinedModeIndexTableNonScanAccept() {
        filter.isIndexTable = true;
        filter.isScanScope = false;
        assertIndexAccept(true, 0L, 0L, Mode.UNDEFINED);
        assertIndexAccept(true, 0L, 1L, Mode.UNDEFINED);
        assertIndexAccept(true, 1L, 0L, Mode.UNDEFINED);
    }

    @Test
    public void undefinedModeNonIndexTableScanAccept() {
        filter.isIndexTable = false;
        filter.isScanScope = true;
        assertNonIndexAccept(false, 0L, 0L, Mode.UNDEFINED);
        assertNonIndexAccept(false, 0L, 1L, Mode.UNDEFINED);
        assertNonIndexAccept(false, 1L, 0L, Mode.UNDEFINED);
    }

    @Test
    public void undefinedModeNonIndexTableNonScanAccept() {
        filter.isIndexTable = false;
        filter.isScanScope = false;
        assertIndexAccept(true, 0L, 0L, Mode.UNDEFINED);
        assertIndexAccept(true, 0L, 1L, Mode.UNDEFINED);
        assertIndexAccept(true, 1L, 0L, Mode.UNDEFINED);
    }

    private void assertIndexAccept(boolean expectedResult, long keyTimestamp, long configuredTimestamp, Mode mode) {
        String dataType = "xyz";
        VersionFilterConfiguration filterConfiguration = createFilterConfiguration(configuredTimestamp, mode);
        filter.dataTypeConfigurations = Collections.singletonMap(new ArrayByteSequence(dataType), filterConfiguration);
        assertEquals(expectedResult, filter.accept(new Key("row", "cf", "0123456789\00"+ dataType, keyTimestamp), new Value()));
    }

    private void assertNonIndexAccept(boolean expectedResult, long keyTimestamp, long configuredTimestamp, Mode mode) {
        String dataType = "xyz";
        VersionFilterConfiguration filterConfiguration = createFilterConfiguration(configuredTimestamp, mode);
        filter.dataTypeConfigurations = Collections.singletonMap(new ArrayByteSequence(dataType), filterConfiguration);
        assertEquals(expectedResult, filter.accept(new Key("row", dataType + "\000123456789", "cq", keyTimestamp), new Value()));
    }

    private VersionFilterConfiguration createFilterConfiguration(long timestampVersion, Mode mode) {
        VersionFilterConfiguration filterConfiguration = new VersionFilterConfiguration();
        filterConfiguration.timestampVersion = timestampVersion;
        filterConfiguration.mode = mode;
        return filterConfiguration;
    }

    @Test
    public void modeParsesFromOptionValue() {
        assertEquals(Mode.EQUAL, Mode.parseOptionValue("eq"));
        assertEquals(Mode.GREATER_THAN_OR_EQUAL, Mode.parseOptionValue("gte"));
        assertEquals(Mode.UNDEFINED, Mode.parseOptionValue("xyz"));
        assertEquals(Mode.UNDEFINED, Mode.parseOptionValue(""));
        assertEquals(Mode.UNDEFINED, Mode.parseOptionValue(null));
    }

    @Test
    public void testInit() throws IOException {
        filter.init(new DevNull(), Collections.emptyMap(), new StubbedIteratorEnvironment());
        assertFalse(filter.isScanScope);
        assertFalse(filter.isIndexTable);
        assertEquals(0, filter.dataTypeConfigurations.size());

        filter.init(new DevNull(), Collections.emptyMap(), new StubbedIteratorEnvironment() {
            @Override
            public IteratorUtil.IteratorScope getIteratorScope() {
                return IteratorUtil.IteratorScope.scan;
            }
        });
        assertTrue(filter.isScanScope);
        assertFalse(filter.isIndexTable);
        assertEquals(0, filter.dataTypeConfigurations.size());

        filter.init(new DevNull(), Collections.emptyMap(), new StubbedIteratorEnvironment() {
            @Override
            public IteratorUtil.IteratorScope getIteratorScope() {
                return IteratorUtil.IteratorScope.majc;
            }
        });
        assertFalse(filter.isScanScope);
        assertFalse(filter.isIndexTable);
        assertEquals(0, filter.dataTypeConfigurations.size());
    }

    // todo split up some use cases
    @Test
    public void configuresDataTypes() {
        Map<String, String> emptyOptions = Collections.emptyMap();
        IteratorEnvironment emptyIterEnv = new StubbedIteratorEnvironment();
        assertEquals(0, filter.gatherDataTypeConfigurations(emptyOptions, emptyIterEnv).size());
        assertEquals(0, filter.gatherDataTypeConfigurations(createDataTypeListOptions(null), emptyIterEnv).size());
        assertEquals(0, filter.gatherDataTypeConfigurations(createDataTypeListOptions(""), emptyIterEnv).size());
        assertEquals(0, filter.gatherDataTypeConfigurations(createDataTypeListOptions(","), emptyIterEnv).size());

        assertSingleDefaultConfig(filter.gatherDataTypeConfigurations(createDataTypeListOptions("abc"), emptyIterEnv));
        assertSingleDefaultConfig(filter.gatherDataTypeConfigurations(createDataTypeListOptions(",abc"), emptyIterEnv));
        assertSingleDefaultConfig(filter.gatherDataTypeConfigurations(createDataTypeListOptions("abc,"), emptyIterEnv));
        assertSingleDefaultConfig(filter.gatherDataTypeConfigurations(createDataTypeListOptions("     , ,abc,"), emptyIterEnv));

        Map<ByteSequence, VersionFilterConfiguration> filters;
        filters = filter.gatherDataTypeConfigurations(createDataTypeListOptions("xyz,abc"), emptyIterEnv);
        assertEquals(2, filters.size());
        assertDefaultConfiguration(filters.get(new ArrayByteSequence("abc")));
        assertDefaultConfiguration(filters.get(new ArrayByteSequence("xyz")));

        Map<String, String> options = new HashMap<>();
        options.put(DATATYPE_LIST_OPTION_NAME, "xyz,abc");
        options.put("abc." + DATATYPE_MODE_OPTION_NAME, "eq");
        options.put("xyz." + DATATYPE_MODE_OPTION_NAME, "gte");
        StubbedIteratorEnvironment iterEnv = getIterEnvWithTimestampStr(Long.toString(10L));
        filters = filter.gatherDataTypeConfigurations(options, iterEnv);
        assertEquals(2, filters.size());
        assertConfiguration(10L, Mode.EQUAL, filters.get(new ArrayByteSequence("abc")));
        assertConfiguration(10L, Mode.GREATER_THAN_OR_EQUAL, filters.get(new ArrayByteSequence("xyz")));
    }

    private void assertSingleDefaultConfig(Map<ByteSequence, VersionFilterConfiguration> filters) {
        assertEquals(1, filters.size());
        assertDefaultConfiguration(filters.get(new ArrayByteSequence("abc")));
    }

    private Map<String, String> createDataTypeListOptions(String value) {
        return Collections.singletonMap(DATATYPE_LIST_OPTION_NAME, value);
    }

    @Test
    public void createsFilterConfiguration() {
        Map<String, String> emptyOptions = Collections.emptyMap();
        IteratorEnvironment emptyIterEnv = new StubbedIteratorEnvironment();
        ByteSequence emptyDataType = new ArrayByteSequence("");
        ByteSequence dataType = new ArrayByteSequence("xyz");

        assertDefaultConfiguration(filter.createFilterConfiguration(emptyOptions, emptyIterEnv, emptyDataType));
        assertDefaultConfiguration(filter.createFilterConfiguration(null, emptyIterEnv, emptyDataType));
        assertDefaultConfiguration(filter.createFilterConfiguration(emptyOptions, null, emptyDataType));
        assertDefaultConfiguration(filter.createFilterConfiguration(emptyOptions, emptyIterEnv, null));

        assertDefaultConfiguration(filter.createFilterConfiguration(emptyOptions, getIterEnvWithTimestampStr(Long.toString(100L)), emptyDataType));
        assertConfiguration(100L, Mode.UNDEFINED, filter.createFilterConfiguration(emptyOptions, getIterEnvWithTimestampStr(Long.toString(100L)), dataType));
        assertConfiguration(100L, Mode.EQUAL, filter.createFilterConfiguration(Collections.singletonMap("xyz." + DATATYPE_MODE_OPTION_NAME, "eq"), getIterEnvWithTimestampStr(Long.toString(100L)), dataType));
        assertConfiguration(100L, Mode.GREATER_THAN_OR_EQUAL, filter.createFilterConfiguration(Collections.singletonMap("xyz." + DATATYPE_MODE_OPTION_NAME, "gte"), getIterEnvWithTimestampStr(Long.toString(100L)), dataType));
        assertConfiguration(100L, Mode.UNDEFINED, filter.createFilterConfiguration(Collections.singletonMap("xyz." + DATATYPE_MODE_OPTION_NAME, "lte"), getIterEnvWithTimestampStr(Long.toString(100L)), dataType));
        assertConfiguration(0L, Mode.UNDEFINED, filter.createFilterConfiguration(Collections.singletonMap("xyz." + DATATYPE_MODE_OPTION_NAME, "lte"), emptyIterEnv, dataType));
    }

    private void assertConfiguration(long timestamp, Mode mode, VersionFilterConfiguration filterConfiguration) {
        assertEquals(timestamp, filterConfiguration.timestampVersion);
        assertEquals(mode, filterConfiguration.mode);
    }

    private void assertDefaultConfiguration(VersionFilterConfiguration filterConfiguration) {
        assertConfiguration(0L, Mode.UNDEFINED, filterConfiguration);
    }

    @Test
    public void parsesTimestamp() {
        IteratorEnvironment iterEnv = new StubbedIteratorEnvironment();
        // verify no exceptions are thrown
        assertEquals(0L, filter.getTimestampVersion(iterEnv, null));
        byte[] emptyBytes = {};
        assertEquals(0L, filter.getTimestampVersion(iterEnv, new ArrayByteSequence(emptyBytes)));
        assertEquals(0L, filter.getTimestampVersion(null, new ArrayByteSequence("xyz")));

        assertEquals(0L, filter.getTimestampVersion(getIterEnvWithTimestampStr("not a number"), new ArrayByteSequence("xyz")));
        assertEquals(0L, filter.getTimestampVersion(getIterEnvWithTimestampStr(Long.toString(Long.MAX_VALUE) + "0"), new ArrayByteSequence("xyz")));
        assertEquals(Long.MAX_VALUE, filter.getTimestampVersion(getIterEnvWithTimestampStr(Long.toString(Long.MAX_VALUE)), new ArrayByteSequence("xyz")));
        assertEquals(Long.MIN_VALUE, filter.getTimestampVersion(getIterEnvWithTimestampStr(Long.toString(Long.MIN_VALUE)), new ArrayByteSequence("xyz")));
        assertEquals(0L, filter.getTimestampVersion(getIterEnvWithTimestampStr(Long.toString(0L)), new ArrayByteSequence("xyz")));
    }

    private StubbedIteratorEnvironment getIterEnvWithTimestampStr(String value) {
        return new StubbedIteratorEnvironment() {
            @Override
            public AccumuloConfiguration getConfig() {
                return new DefaultConfiguration() {
                    @Override
                    public String get(String property) {
                        return value;
                    }
                };
            }
        };
    }

    @Test
    public void parsesIndexTableStatusAsExpected() {
        // option is present
        assertTrue(filter.extractIndexTableStatus(optionsMapWithIsIndexTable(Boolean.TRUE.toString()), null));
        assertFalse(filter.extractIndexTableStatus(optionsMapWithIsIndexTable(Boolean.FALSE.toString()), null));
        assertFalse(filter.extractIndexTableStatus(Collections.singletonMap(IS_INDEX_TABLE_OPTION_NAME, "bogus"), null));
        assertFalse(filter.extractIndexTableStatus(Collections.singletonMap(IS_INDEX_TABLE_OPTION_NAME, null), null));
        assertTrue("options precede table config", filter.extractIndexTableStatus(optionsMapWithIsIndexTable(Boolean.TRUE.toString()), createIterEnvWithIsIndexTableValue(Boolean.FALSE.toString())));
        assertFalse("options precede table config", filter.extractIndexTableStatus(optionsMapWithIsIndexTable(Boolean.FALSE.toString()), createIterEnvWithIsIndexTableValue(Boolean.TRUE.toString())));

        // option is missing
        assertFalse("should tolerate null value for iterEnv", filter.extractIndexTableStatus(Collections.emptyMap(), null));
        assertFalse("should tolerate null value for iterEnv.config()", filter.extractIndexTableStatus(Collections.emptyMap(), new StubbedIteratorEnvironment()));
        assertFalse("should tolerate null value for table property value", filter.extractIndexTableStatus(Collections.emptyMap(), createIterEnvWithIsIndexTableValue(null)));
        assertFalse("should tolerate invalid value for table property value", filter.extractIndexTableStatus(Collections.emptyMap(), createIterEnvWithIsIndexTableValue("bogus")));
        assertTrue(filter.extractIndexTableStatus(Collections.emptyMap(), createIterEnvWithIsIndexTableValue(Boolean.TRUE.toString())));
    }

    private StubbedIteratorEnvironment createIterEnvWithIsIndexTableValue(String value) {
        return new StubbedIteratorEnvironment() {
            @Override
            public AccumuloConfiguration getConfig() {
                return new DefaultConfiguration() {
                    @Override
                    public String get(String property) {
                        return value;
                    }
                };
            }
        };
    }

    private Map<String, String> optionsMapWithIsIndexTable(String value) {
        return Collections.singletonMap(IS_INDEX_TABLE_OPTION_NAME, value);
    }

    @Test
    public void init() {
    }

    @Test
    public void deepCopy() {
    }

    @Test
    public void describeOptions() {
    }

    @Test
    public void validateOptions() {
    }
}