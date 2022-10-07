package datawave.query.transformer;

import com.google.common.base.Function;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.data.Key;

import javax.annotation.Nullable;
import java.util.Map;

public interface DocumentTransform extends Function<Map.Entry<Key,Document>,Map.Entry<Key,Document>> {
    // called when adding the document transform
    void initialize(Query settings, MarkingFunctions markingFunctions);
    
    // called after the last document is passed through to get any remaining aggregated results.
    Map.Entry<Key,Document> flush();
    
    /**
     * Some transformers (GroupingTransform) have logic that is predicated on knowing when a page of results is starting to be processed. This sets the time
     * that a page has begun processing (in milliseconds)
     *
     * @param queryExecutionForPageStartTime
     */
    void setQueryExecutionForPageStartTime(long queryExecutionForPageStartTime);
    
    class DefaultDocumentTransform implements DocumentTransform {
        protected Query settings;
        protected MarkingFunctions markingFunctions;
        protected long queryExecutionForPageStartTime;
        
        @Override
        public void initialize(Query settings, MarkingFunctions markingFunctions) {
            this.settings = settings;
            this.markingFunctions = markingFunctions;
        }
        
        @Override
        public Map.Entry<Key,Document> flush() {
            return null;
        }
        
        @Nullable
        @Override
        public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> keyDocumentEntry) {
            return keyDocumentEntry;
        }
        
        @Override
        public void setQueryExecutionForPageStartTime(long queryExecutionForPageStartTime) {
            this.queryExecutionForPageStartTime = queryExecutionForPageStartTime;
        }
    }
    
}
