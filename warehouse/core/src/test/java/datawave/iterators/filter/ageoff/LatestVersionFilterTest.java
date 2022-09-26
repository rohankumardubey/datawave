package datawave.iterators.filter.ageoff;

import org.junit.Test;

import static org.junit.Assert.*;

public class LatestVersionFilterTest {

    @Test
    public void modeParsesFromOptionValue() {
        assertEquals(LatestVersionFilter.Mode.EQUAL, LatestVersionFilter.Mode.parseOptionValue("eq"));
        assertEquals(LatestVersionFilter.Mode.GREATER_THAN_OR_EQUAL, LatestVersionFilter.Mode.parseOptionValue("gte"));
        assertEquals(LatestVersionFilter.Mode.UNDEFINED, LatestVersionFilter.Mode.parseOptionValue("xyz"));
        assertEquals(LatestVersionFilter.Mode.UNDEFINED, LatestVersionFilter.Mode.parseOptionValue(""));
        assertEquals(LatestVersionFilter.Mode.UNDEFINED, LatestVersionFilter.Mode.parseOptionValue(null));
    }


    // todo - write tests

    @Test
    public void accept() {
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