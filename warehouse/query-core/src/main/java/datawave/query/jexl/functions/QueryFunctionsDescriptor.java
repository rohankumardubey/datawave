package datawave.query.jexl.functions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

public class QueryFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {
    
    public static final String BETWEEN = "between";
    public static final String LENGTH = "length";
    public static final String INCLUDE_TEXT = "includeText";
    
    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     */
    public static class QueryJexlArgumentDescriptor implements JexlArgumentDescriptor {
        private final ASTFunctionNode node;
        private final String namespace, name;
        private final List<JexlNode> args;
        
        public QueryJexlArgumentDescriptor(ASTFunctionNode node, String namespace, String name, List<JexlNode> args) {
            this.node = node;
            this.namespace = namespace;
            this.name = name;
            this.args = args;
        }
        
        @Override
        public JexlNode getIndexQuery(ShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper, Set<String> datatypeFilter) {
            switch (name) {
                case BETWEEN:
                    JexlNode geNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), args.get(1).image);
                    JexlNode leNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), args.get(2).image);
                    // Return a bounded range.
                    return BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geNode, leNode)));
                case LENGTH:
                    // Return a regex node with the appropriate number of matching characters
                    return JexlNodeFactory.buildNode(new ASTERNode(ParserTreeConstants.JJTERNODE), args.get(0), ".{" + args.get(1).image + ','
                                    + args.get(2).image + '}');
                case QueryFunctions.MATCH_REGEX:
                    // Return an index query.
                    return getIndexQuery();
                case INCLUDE_TEXT:
                    // Return the appropriate index query.
                    return getTextIndexQuery();
                default:
                    // Return the true node if unable to parse arguments.
                    return TRUE_NODE;
            }
        }
        
        private JexlNode getIndexQuery() {
            JexlNode node0 = args.get(0);
            final String value = args.get(1).image;
            if (node0 instanceof ASTIdentifier) {
                final String field = JexlASTHelper.deconstructIdentifier(node0.image);
                return JexlNodeFactory.buildNode((ASTERNode) null, field, value);
            } else {
                // node0 is an Or node or an And node
                // copy it
                JexlNode newParent = JexlNodeFactory.shallowCopy(node0);
                int i = 0;
                for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(node0)) {
                    String field = JexlASTHelper.deconstructIdentifier(identifier.image);
                    JexlNode kid = JexlNodeFactory.buildNode((ASTERNode) null, field, value);
                    kid.jjtSetParent(newParent);
                    newParent.jjtAddChild(kid, i++);
                }
                return newParent;
            }
        }
        
        private JexlNode getTextIndexQuery() {
            JexlNode node0 = args.get(0);
            final String value = args.get(1).image;
            if (node0 instanceof ASTIdentifier) {
                final String field = JexlASTHelper.deconstructIdentifier(node0.image);
                return JexlNodeFactory.buildNode((ASTEQNode) null, field, value);
            } else {
                // node0 is an Or node or an And node
                // copy it
                JexlNode newParent = JexlNodeFactory.shallowCopy(node0);
                int i = 0;
                for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(node0)) {
                    String field = JexlASTHelper.deconstructIdentifier(identifier.image);
                    JexlNode kid = JexlNodeFactory.buildNode((ASTEQNode) null, field, value);
                    kid.jjtSetParent(newParent);
                    newParent.jjtAddChild(kid, i++);
                }
                return newParent;
            }
        }
        
        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
            // noop, covered by getIndexQuery (see comments on interface)
        }
        
        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            // Do not normalize fields for the includeText function.
            if (!name.equalsIgnoreCase(INCLUDE_TEXT)) {
                // All other functions use the fields in the first argument for normalization.
                if (arg > 0) {
                    return fields(helper, datatypeFilter);
                }
            }
            return Collections.emptySet();
        }
        
        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            return JexlASTHelper.getIdentifierNames(args.get(0));
        }
        
        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            return JexlArgumentDescriptor.Fields.product(args.get(0));
        }
        
        @Override
        public boolean useOrForExpansion() {
            return true;
        }
        
        @Override
        public boolean regexArguments() {
            return true;
        }
        
        @Override
        public boolean allowIvaratorFiltering() {
            return true;
        }
    }
    
    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        FunctionJexlNodeVisitor visitor = FunctionJexlNodeVisitor.eval(node);
        Class<?> functionClass = (Class<?>) ArithmeticJexlEngines.functions().get(visitor.namespace());
        
        if (!QueryFunctions.QUERY_FUNCTION_NAMESPACE.equals(node.jjtGetChild(0).image))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with an unexpected namespace of "
                            + node.jjtGetChild(0).image);
        if (!functionClass.equals(QueryFunctions.class))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with node for a function in "
                            + functionClass);
        
        verify(visitor.name(), visitor.args().size());
        
        return new QueryJexlArgumentDescriptor(node, visitor.namespace(), visitor.name(), visitor.args());
    }
    
    private static void verify(String name, int numArgs) {
        switch (name) {
            case BETWEEN:
                if (numArgs != 3) {
                    throw new IllegalArgumentException("Wrong number of arguments to between function");
                }
                break;
            case LENGTH:
                if (numArgs != 3) {
                    throw new IllegalArgumentException("Wrong number of arguments to length function");
                }
                break;
            case QueryFunctions.OPTIONS_FUNCTION:
                if (numArgs % 2 != 0) {
                    throw new IllegalArgumentException("Expected even number of arguments to options function");
                }
                break;
            case QueryFunctions.UNIQUE_FUNCTION:
            case QueryFunctions.UNIQUE_BY_DAY_FUNCTION:
            case QueryFunctions.UNIQUE_BY_HOUR_FUNCTION:
            case QueryFunctions.UNIQUE_BY_MINUTE_FUNCTION:
            case QueryFunctions.GROUPBY_FUNCTION:
            case QueryFunctions.EXCERPT_FIELDS_FUNCTION:
            case QueryFunctions.MATCH_REGEX:
            case QueryFunctions.INCLUDE_TEXT:
            case QueryFunctions.NO_EXPANSION:
                if (numArgs == 0) {
                    throw new IllegalArgumentException("Expected at least one argument to the " + name + " function");
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown Query function: " + name);
        }
    }
}
