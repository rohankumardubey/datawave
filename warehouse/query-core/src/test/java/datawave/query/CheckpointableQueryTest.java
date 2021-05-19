package datawave.query;

import datawave.query.tables.ShardQueryLogic;
import org.apache.log4j.Logger;

public class CheckpointableQueryTest extends AnyFieldQueryTest {
    
    private static final Logger log = Logger.getLogger(CheckpointableQueryTest.class);
    
    @Override
    protected ShardQueryLogic createShardQueryLogic() {
        ShardQueryLogic logic = super.createShardQueryLogic();
        logic.setCheckpointable(true);
        return logic;
    }
}