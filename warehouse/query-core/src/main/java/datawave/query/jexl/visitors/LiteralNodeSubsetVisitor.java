package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;

import java.util.Set;

/**
 * Collect a {@link Set} of all literals/terms in the AST that belong to the specified set of fields.
 */
@SuppressWarnings("unchecked")
public class LiteralNodeSubsetVisitor extends ShortCircuitBaseVisitor {
    
    public static Multimap<String,String> getLiterals(Set<String> expectedFields, ASTJexlScript script) {
        LiteralNodeSubsetVisitor visitor = new LiteralNodeSubsetVisitor(expectedFields);
        
        return (Multimap<String,String>) script.jjtAccept(visitor, HashMultimap.create());
    }
    
    protected final Set<String> expectedFields;
    
    public LiteralNodeSubsetVisitor(Set<String> expectedFields) {
        this.expectedFields = expectedFields;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        Multimap<String,String> literals = (Multimap<String,String>) data;
        
        String identifier = JexlASTHelper.getIdentifier(node);
        if (expectedFields.contains(identifier)) {
            Object literal = JexlASTHelper.getLiteralValue(node);
            literals.put(identifier, (literal == null ? null : literal.toString()));
        }
        
        return literals;
    }
    
    // Descend through these nodes
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
}
