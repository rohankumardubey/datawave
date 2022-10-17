package datawave.iterators.filter.ageoff;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import datawave.iterators.filter.AgeOffConfigParams;

import datawave.util.StringUtils;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

/**
 * This filter is designed to optionally support filtering for dataTypes against a configured timestamp version.
 *
 * If filtering is enabled for a dataType, there are two modes:
 * @see Mode#EQUAL (configured with 'eq')  only show data if it exactly matches its dataType's configured timestamp
 * @see Mode#GREATER_THAN_OR_EQUAL (configured with 'gte') show data if its timestamp is greater than or equal to its dataType's configured timestamp
 *
 * This filter reads the iterator options and table properties to determine:
 * - a comma separated list of dataTypes to filter, via the iterator options property name of 'opt.dataTypes'
 * - the mode of filtering for each filtered dataType, via the iterator options property name of 'opt.mode.(dataType)'
 * - the timestamp version for each filtered dataType, via custom.table property of 'table.custom.timestamp.current.(dataType)'
 * - if the table is an index table or not, for parsing dataTypes from keys, via either iterator options property name 'opt.isindextable' or custom.table property 'table.custom.isindextable'
 *
 * During evaluation, this filter parses the key to identify its dataType.
 *
 * If a dataType is configured to be filtered but has no configured timestamp, it will use a default timestamp of 0.
 *
 * If a dataType is configured to be filtered but has no configured mode, it will default to reject all data for Scan and accept all data for Compaction scopes.
 *
 */
public class LatestVersionFilter extends Filter {
    private static final Logger log = Logger.getLogger(LatestVersionFilter.class);

    static final String DATATYPE_LIST_OPTION_NAME = "dataTypes";
    static final String DATATYPE_MODE_OPTION_NAME = "mode";
    private static final String DATATYPE_TIMESTAMP_PROPERTY_NAME_PREFIX = "table.custom.timestamp.current.";
    private static final String IS_INDEX_TABLE_PROPERTY_NAME = "table.custom." + AgeOffConfigParams.IS_INDEX_TABLE;
    static final String IS_INDEX_TABLE_OPTION_NAME = AgeOffConfigParams.IS_INDEX_TABLE;

    // Index table keys are parsed differently to extract the DataType
    protected boolean isIndexTable;

    // Maps: DataType -> Configuration for filtering logic
    protected Map<ByteSequence, VersionFilterConfiguration> dataTypeConfigurations = null;

    // Iterator Scope used only in failure case to protect data
    @VisibleForTesting
    boolean isScanScope;

    public LatestVersionFilter() {
        super();
    }
    // todo test
    /**
     * @param key parsed for dataType, timestamp examined
     * @param value ignored
     * @return true if the key's timestamp conforms to the filterConfiguration for its datatype
     */
    @Override
    public boolean accept(Key key, Value value) {
        // parse dataType from key
        ByteSequence dataType = DataTypeParser.parseKey(key, this.isIndexTable);

        // determine if dataType is configured for filtering
        VersionFilterConfiguration filterConfiguration = dataTypeConfigurations.get(dataType);

        // if dataType is not configured for filtering, just accept the key / value
        if (null == filterConfiguration) {
            return true;
        }

        // evaluate key's timestamp version against configuration
        return accept(key.getTimestamp(), filterConfiguration);
    }

    // todo test
    /**
     * @param dataTimestamp the timestamp version for the key under evaluation
     * @param filterConfiguration a non-null filterConfiguration
     * @return whether to accept or reject this timestamp
     */
    private boolean accept(long dataTimestamp, VersionFilterConfiguration filterConfiguration) {
        // determine dataType's configured timestamp
        long configuredTimestamp = filterConfiguration.timestampVersion;

        // apply logic for configured mode
        switch (filterConfiguration.mode) {
            case EQUAL:
                return dataTimestamp == configuredTimestamp;
            case GREATER_THAN_OR_EQUAL:
                return dataTimestamp >= configuredTimestamp;
            case UNDEFINED:
            default:
                log.warn("DataType {} was not configured for 'eq' or 'gte'");
                // By default: accept all data at compaction scopes, no data at scan scope
                return !this.isScanScope;
        }
    }

    // todo test
    /**
     * @param source simply passed to super.init
     * @param options examined to create filterConfiguration, isIndexTable
     * @param iterEnv examined to create filterConfiguration, isIndexTable, isScanScope
     * @throws IOException only expected from super.init
     */
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment iterEnv) throws IOException {
        super.init(source, options, iterEnv);

        // determine if this is operating at scan
        this.isScanScope = (IteratorUtil.IteratorScope.scan == iterEnv.getIteratorScope());

        this.isIndexTable = extractIndexTableStatus(options, iterEnv);

        this.dataTypeConfigurations = gatherDataTypeConfigurations(options, iterEnv);
    }

    /**
     * Examines the Map of iterator options and IteratorEnvironment to create a mapping between data types and their filter configurations
     * @param options iterator options
     * @param iterEnv iterator environment
     * @return map containing dataType -> filter configuration for its data
     */
    @VisibleForTesting
    Map<ByteSequence, VersionFilterConfiguration> gatherDataTypeConfigurations(Map<String, String> options, IteratorEnvironment iterEnv) {
        Map<ByteSequence, VersionFilterConfiguration> configurationsMap = new HashMap<>();

        String dataTypeListOptionValue = options.get(DATATYPE_LIST_OPTION_NAME);
        if (Strings.isNullOrEmpty(dataTypeListOptionValue)) {
            log.trace(DATATYPE_LIST_OPTION_NAME + " is null or empty.  Configure data type list.");
            return configurationsMap;
        }

        String[] dataTypes = StringUtils.split(dataTypeListOptionValue, ",");
        for (String dataTypeStr : dataTypes) {
            if (null == dataTypeStr || dataTypeStr.trim().length() == 0) {
                log.warn("Invalid dataType found in " + DATATYPE_LIST_OPTION_NAME + "=" + dataTypeListOptionValue);
                continue;
            }

            ByteSequence dataType = new ArrayByteSequence(dataTypeStr.trim().getBytes());
            VersionFilterConfiguration filterConfiguration = createFilterConfiguration(options, iterEnv, dataType);
            configurationsMap.put(dataType, filterConfiguration);
        }

        return configurationsMap;
    }

    /**
     * @param options iterator options
     * @param iterEnv iterator environment
     * @param dataType data type to configure
     * @return the version filter configuration for the provided data type
     */
    @VisibleForTesting
    VersionFilterConfiguration createFilterConfiguration(Map<String, String> options, IteratorEnvironment iterEnv, ByteSequence dataType) {
        VersionFilterConfiguration filterConfiguration = new VersionFilterConfiguration();

        // set mode
        String dataTypeMode = (null != options) ? options.get(dataType + "." + DATATYPE_MODE_OPTION_NAME) : null;
        filterConfiguration.mode = Mode.parseOptionValue(dataTypeMode);

        // set version timestamp
        filterConfiguration.timestampVersion = getTimestampVersion(iterEnv, dataType);

        return filterConfiguration;
    }

    /**
     * @param iterEnv iterator environment
     * @param dataType data type to lookup
     * @return timestamp version for provided datatype, in millis since epoch
     */
    long getTimestampVersion(IteratorEnvironment iterEnv, ByteSequence dataType) {
        if (null == dataType || 0 == dataType.length()) {
            log.warn("dataType is null or empty");
            return 0;
        } else if (null == iterEnv || null == iterEnv.getConfig()) {
            log.warn("Problem evaluating AccumuloConfiguration");
            return 0;
        }

        String timestampVersionStr = iterEnv.getConfig().get(DATATYPE_TIMESTAMP_PROPERTY_NAME_PREFIX + dataType);
        if (Strings.isNullOrEmpty(timestampVersionStr)) {
            log.warn("No AccumuloConfiguration property value found in IteratorEnvironment for " + DATATYPE_TIMESTAMP_PROPERTY_NAME_PREFIX + dataType);
        } else {
            try {
                return Long.parseLong(timestampVersionStr, 10);
            } catch (final NumberFormatException e) {
                log.warn("Could not parse " + DATATYPE_TIMESTAMP_PROPERTY_NAME_PREFIX + dataType + "=" + timestampVersionStr);
            }
        }
        return 0; // default
    }

    /**
     * @param options options for the iterator
     * @param iterEnv iterator environment which includes an Accumulo Configuration with table properties
     * @return true if either the options or the iteratorEnvironment configuration indicate the filter is examining an index table
     */
    @VisibleForTesting
    boolean extractIndexTableStatus(Map<String, String> options, IteratorEnvironment iterEnv) {
        if (options.get(IS_INDEX_TABLE_OPTION_NAME) != null) {
            return Boolean.valueOf(options.get(IS_INDEX_TABLE_OPTION_NAME));
        } else if (iterEnv != null && iterEnv.getConfig() != null) {
            return Boolean.parseBoolean(iterEnv.getConfig().get(IS_INDEX_TABLE_PROPERTY_NAME));
        }
        return false;
    }

    // todo test
    /**
     * @param env iterator environment
     * @return a copy of LatestVersionFilter with all the same field values
     */
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        // todo - verify deepCopy cannot be called for a different iterator scope
        LatestVersionFilter copy = (LatestVersionFilter) super.deepCopy(env);
        copy.isIndexTable = this.isIndexTable;
        copy.dataTypeConfigurations = this.dataTypeConfigurations;
        copy.isScanScope = this.isScanScope;
        return copy;
    }

    // todo test
    /**
     * @return iterator options for this Filter
     */
    @Override
    public OptionDescriber.IteratorOptions describeOptions() {
        IteratorOptions iteratorOptions = new IteratorOptions(getClass().getSimpleName(),
                "Filters keys according to configurable timestamp version and comparison mode", null, null);
        iteratorOptions.addNamedOption(DATATYPE_LIST_OPTION_NAME, "Comma separated list of case-sensitive dataTypes");
        iteratorOptions.addNamedOption(DATATYPE_MODE_OPTION_NAME, "The comparison mode to use for a dataType, 'eq' for equals (exact timestamp match), 'gte' for greater-than-or-equal");
        iteratorOptions.addNamedOption(IS_INDEX_TABLE_OPTION_NAME, "Boolean indicating if this is an index table, default false");
        // Looking for DATATYPE_TIMESTAMP_PROPERTY_NAME or IS_INDEX_TABLE_PROPERTY_NAME?  They are not iterator options.
        // They are retrieved from the AccumuloConfiguration via the IteratorEnvironment.

// todo - remove these two commented lines after confirming through testing that they are not needed.
//        opts.addNamedOption(DATATYPE_TIMESTAMP_PROPERTY_NAME, "The version timestamp to use for a given dataType");
//        opts.addNamedOption(IS_INDEX_TABLE_PROPERTY_NAME, "Boolean indicating if this is an index table, default false");

        return iteratorOptions;
    }

    // todo test
    /**
     * @param configuredOptionsMap the iterator options map
     * @return true iff the optional properties are valid when set
     */
    @Override
    public boolean validateOptions(Map<String, String> configuredOptionsMap) {
        return validateIsIndexTableOption(configuredOptionsMap) && validateDataTypeListAndModes(configuredOptionsMap);
    }

    // todo test
    /**
     * @param configuredOptionsMap the iterator options map
     * @return true iff the is index table value, when present, is a parseable boolean
     */
    private boolean validateIsIndexTableOption(Map<String, String> configuredOptionsMap) {
        if (configuredOptionsMap.containsKey(IS_INDEX_TABLE_OPTION_NAME)) {
            try {
                // verify it can be parsed
                Boolean.parseBoolean(configuredOptionsMap.get(IS_INDEX_TABLE_OPTION_NAME));
            } catch (Exception exception) {
                log.warn("Failed to parse boolean " + IS_INDEX_TABLE_OPTION_NAME + "=" + configuredOptionsMap.get(IS_INDEX_TABLE_OPTION_NAME));
                return true;
            }
        }
        return false;
    }

    // todo test
    /**
     * @param configuredOptionsMap the iterator options map
     * @return true iff the datatype list is non-empty, all of its values are non-empty and all have a valid mode
     */
    private boolean validateDataTypeListAndModes(Map<String, String> configuredOptionsMap) {
        String[] dataTypes;

        // verify the data type list is present and can be split on commas
        String dataTypeListOptionValue = configuredOptionsMap.get(DATATYPE_LIST_OPTION_NAME);
        if (Strings.isNullOrEmpty(dataTypeListOptionValue)) {
            log.warn(DATATYPE_LIST_OPTION_NAME + " is null or empty.  Configure data type list.");
            return false;
        } else {
            try {
                dataTypes = StringUtils.split(dataTypeListOptionValue, ",");
            } catch (Exception exception) {
                log.warn("Error splitting comma separated list " + DATATYPE_LIST_OPTION_NAME + "=" + dataTypeListOptionValue);
                return false;
            }
        }

        if (dataTypes.length == 0) {
            log.warn(DATATYPE_LIST_OPTION_NAME + " has no values.");
            return false;
        }

        for (String dataType : dataTypes) {
            if (Strings.isNullOrEmpty(dataType)) {
                log.warn("One of the data types appears to be empty " + DATATYPE_LIST_OPTION_NAME + "=" + dataTypeListOptionValue);
                return false;
            }
            // If a mode is provided, ensure it's valid
            String dataTypeMode = configuredOptionsMap.get(DATATYPE_MODE_OPTION_NAME);
            if (!Strings.isNullOrEmpty(dataTypeMode) && Mode.UNDEFINED == Mode.parseOptionValue(dataTypeMode)) {
                log.warn("Could not parse " + DATATYPE_MODE_OPTION_NAME + "=" + dataTypeMode);
                return false;
            }
        }
        return true;
    }

    /**
     * Encapsulates the information for the filtering logic for a data type
     */
@VisibleForTesting
static
    class VersionFilterConfiguration {
        public long timestampVersion;
        public Mode mode;
    }

    /**
     * Enumerates the available modes for filtering and also defines the short form name for each.
     */
    @VisibleForTesting
    enum Mode {
        EQUAL("eq"),
        GREATER_THAN_OR_EQUAL("gte"),
        UNDEFINED(null);

        private final String optionValue;

        Mode(String optionValue) {
            this.optionValue = optionValue;
        }

        public static Mode parseOptionValue(String value) {
            for (Mode mode : Mode.values()) {
                if (mode != UNDEFINED && mode.optionValue.equals(value)) {
                    return mode;
                }
                boolean x= false;
            }
            return UNDEFINED;
        }
    }
}
