package datawave.query.tables;

import datawave.webservice.common.remote.RemoteQueryService;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class RemoteEventQueryLogicTest {
    
    RemoteEventQueryLogic logic = new RemoteEventQueryLogic();
    
    @Before
    public void setup() {
        UUID uuid = UUID.randomUUID();
        GenericResponse<String> createResponse = new GenericResponse<String>();
        createResponse.setResult(uuid.toString());
        
        DefaultEventQueryResponse response1 = new DefaultEventQueryResponse();
        DefaultEvent event1 = new DefaultEvent();
        event1.setFields(Collections.singletonList(new DefaultField("FOO1", "FOO|BAR", new HashMap(), -1L, "FOOBAR1")));
        response1.setEvents(Collections.singletonList(event1));
        response1.setReturnedEvents(1L);
        
        DefaultEventQueryResponse response2 = new DefaultEventQueryResponse();
        DefaultEvent event2 = new DefaultEvent();
        event1.setFields(Collections.singletonList(new DefaultField("FOO2", "FOO|BAR", new HashMap(), -1L, "FOOBAR2")));
        response2.setEvents(Collections.singletonList(event1));
        response2.setReturnedEvents(1L);
        
        // create a remote event query logic that has our own remote query service behind it
        logic.setRemoteQueryService(new TestRemoteQueryService(createResponse, response1, response2));
        logic.setRemoteQueryLogic("TestQuery");
    }
    
    @Test
    public void testRemoteQuery() throws Exception {
        GenericQueryConfiguration config = logic.initialize(null, new QueryImpl(), null);
        logic.setupQuery(config);
        Iterator<EventBase> t = logic.iterator();
        List<EventBase> events = new ArrayList();
        while (t.hasNext()) {
            events.add(t.next());
        }
        assertEquals(2, events.size());
    }
    
    public static class TestRemoteQueryService implements RemoteQueryService {
        GenericResponse<String> createResponse;
        LinkedList<BaseQueryResponse> nextResponses;
        
        public TestRemoteQueryService(GenericResponse<String> createResponse, BaseQueryResponse response1, BaseQueryResponse response2) {
            this.createResponse = createResponse;
            this.nextResponses = new LinkedList<>();
            nextResponses.add(response1);
            nextResponses.add(response2);
        }
        
        @Override
        public GenericResponse<String> createQuery(String queryLogicName, Map<String,List<String>> queryParameters, Object callerObject) {
            return createResponse;
        }
        
        @Override
        public BaseQueryResponse next(String id, Object callerObject) {
            return nextResponses.poll();
        }
        
        @Override
        public VoidResponse close(String id, Object callerObject) {
            return new VoidResponse();
        }
        
        @Override
        public GenericResponse<String> planQuery(String queryLogicName, Map<String,List<String>> queryParameters, Object callerObject) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public GenericResponse<String> planQuery(String id, Object callerObject) {
            throw new UnsupportedOperationException();
        }
    }
}
