package datawave.query.iterator.logic;

import com.google.common.collect.TreeMultimap;
import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Performs a deduping merge of iterators.
 *
 * 
 * @param <T>
 */
public class OrIterator<T extends Comparable<T>> implements NestedIterator<T> {
    // temporary stores of uninitialized streams of iterators
    private List<NestedIterator<T>> includes, contextIncludes, contextExcludes;
    
    private Map<T,T> transforms;
    private Util.Transformer<T> transformer;
    
    private TreeMultimap<T,NestedIterator<T>> includeHeads, contextIncludeHeads, contextIncludeNullHeads, contextExcludeHeads, contextExcludeNullHeads,
                    includeHints;
    
    private T prev;
    private T next;
    
    private Document prevDocument, document;
    
    private T evaluationContext;
    
    private boolean converged = false;
    
    public OrIterator(Iterable<NestedIterator<T>> sources) {
        this(sources, null);
    }
    
    public OrIterator(Iterable<NestedIterator<T>> sources, Iterable<NestedIterator<T>> filters) {
        includes = new LinkedList<>();
        contextIncludes = new LinkedList<>();
        for (NestedIterator<T> src : sources) {
            if (src.isContextRequired()) {
                contextIncludes.add(src);
            } else {
                includes.add(src);
            }
        }
        
        if (filters == null) {
            contextExcludes = Collections.emptyList();
        } else {
            contextExcludes = new LinkedList<>();
            for (NestedIterator<T> filter : filters) {
                contextExcludes.add(filter);
            }
        }
    }
    
    /**
     * Allows creators of this iterator to defer creating the sorted mapping of values to iterators until some condition is met. This is intended to let us
     * build the tree of iterators in <code>init()</code> and defer sorting the iterators until after <code>seek()</code> is called.
     */
    public void initialize() {
        Comparator<T> keyComp = Util.keyComparator();
        // nestedIteratorComparator will keep a deterministic ordering, unlike hashCodeComparator
        Comparator<NestedIterator<T>> itrComp = Util.nestedIteratorComparator();
        
        transformer = Util.keyTransformer();
        transforms = new HashMap<>();
        
        includeHeads = TreeMultimap.create(keyComp, itrComp);
        includeHints = TreeMultimap.create(keyComp, itrComp);
        initSubtree(includeHints, includes, transformer, transforms, false);
        
        if (contextIncludes.size() > 0) {
            contextIncludeHeads = TreeMultimap.create(keyComp, itrComp);
            contextIncludeNullHeads = TreeMultimap.create(keyComp, itrComp);
        }
        
        if (contextExcludes.size() > 0) {
            contextExcludeHeads = TreeMultimap.create(keyComp, itrComp);
            contextExcludeNullHeads = TreeMultimap.create(keyComp, itrComp);
        }
        
        // do not converge
    }
    
    public boolean hasNext() {
        if (null == includeHeads) {
            throw new IllegalStateException("initialize() was never called");
        }
        
        if (!converged) {
            if (includeHints.size() > 0) {
                converge(includeHints, transforms, includeHints.keySet().first(), includeHeads);
            }
            
            converged = true;
            next();
        }
        
        return next != null;
    }
    
    /**
     * return the previously found next and set its document. If there are more head references, get the lowest, advancing all iterators tied to lowest and set
     * next/document for the next call
     * 
     * @return the previously found next
     */
    public T next() {
        if (isContextRequired() && evaluationContext == null) {
            throw new IllegalStateException("evaluationContext must be set prior to calling next");
        }
        
        prev = next;
        prevDocument = document;
        
        if (converged) {
            SortedSet<T> candidateSet = new TreeSet<>(Util.keyComparator());
            T lowest;
            if (includeHeads.keySet().size() > 0) {
                lowest = includeHeads.keySet().first();
                candidateSet.add(lowest);
            }
            
            T lowestContextInclude = null;
            if (evaluationContext != null) {
                if (contextIncludes.size() > 0) {
                    // get the lowest union and add it for contextRequiredIncludes
                    lowestContextInclude = NestedIteratorContextUtil.union(evaluationContext, contextIncludes, contextIncludeHeads, contextIncludeNullHeads,
                                    transformer);
                    if (lowestContextInclude != null) {
                        candidateSet.add(lowestContextInclude);
                    }
                }
                
                if (contextExcludes.size() > 0) {
                    // DeMorgan's Law: (~A) OR (~B) == ~(A AND B)
                    // for an exclude intersect the evaluation context with the set and then as long as the result doesn't match it is a candidate
                    T intersectExclude = NestedIteratorContextUtil.intersect(evaluationContext, contextExcludes, contextExcludeHeads, contextExcludeNullHeads,
                                    transformer);
                    if (!evaluationContext.equals(intersectExclude)) {
                        candidateSet.add(evaluationContext);
                    }
                }
            }
            
            // take the lowest of the candidates
            if (candidateSet.size() > 0) {
                lowest = candidateSet.first();
                
                // decide how to construct the document
                if (lowest.equals(lowestContextInclude)) {
                    // build it from the contextIncludeHeads
                    next = lowestContextInclude;
                    document = Util.buildNewDocument(contextIncludeHeads.get(next));
                } else if (includeHeads.keySet().size() > 0 && lowest.equals(includeHeads.keySet().first())) {
                    // build it from the includeHeads
                    next = transforms.get(lowest);
                    document = Util.buildNewDocument(includeHeads.get(lowest));
                } else {
                    // nothing to build it from all we know is that it wasn't in the exclude set
                    next = evaluationContext;
                    document = Util.buildNewDocument(Collections.emptyList());
                }
                
                // regardless of where we hit make sure to advance includeHeads if it matches there
                if (includeHeads.containsKey(lowest)) {
                    includeHints = hintIterators(lowest);
                    converged = false;
                }
            }
        }
        
        // the loop couldn't find a new next, so set next to null because we're done after this
        if (prev == next) {
            next = null;
        }
        
        return prev;
    }
    
    /**
     * Test all layers of cache for the minimum, then if necessary advance heads
     * 
     * @param minimum
     *            the minimum to return
     * @return the first greater than or equal to minimum or null if none exists
     * @throws IllegalStateException
     *             if prev is greater than or equal to minimum
     */
    public T move(T minimum) {
        if (null == includeHeads) {
            throw new IllegalStateException("initialize() was never called");
        }
        
        // test preconditions
        if (prev != null && prev.compareTo(minimum) >= 0) {
            throw new IllegalStateException("Tried to call move when already at or beyond move point: topkey=" + prev + ", movekey=" + minimum);
        }
        
        // test if the cached next is already beyond the minimum
        if (next != null && next.compareTo(minimum) >= 0) {
            // simply advance to next
            return next();
        }

        // initially converge to populate includeHeads based on the minimum
        if (!converged) {
            converge(includeHints, transforms, minimum, includeHeads);
            converged = true;
        }
        
        Set<T> headSet = includeHeads.keySet().headSet(minimum);
        
        // some iterators need to be moved into the target range before recalculating the next
        Iterator<T> topKeys = new LinkedList<>(headSet).iterator();
        boolean moved = false;
        while (!includeHeads.isEmpty() && topKeys.hasNext()) {
            // advance each iterator that is under the threshold
            includeHeads = moveIterators(topKeys.next(), minimum);
            moved = true;
        }
        
        // after moving the heads, ensure that anything still in hints is great than the new minimum otherwise converge again
        if (moved && includeHints.size() > 0 && includeHeads.keySet().first().compareTo(includeHints.keySet().first()) >= 0) {
            // converge the hints again to whichever is smaller, the minimum or the first from the includeHeads
            if (minimum.compareTo(includeHeads.keySet().first()) > 0) {
                minimum = includeHeads.keySet().first();
            }
            converge(includeHints, transforms, minimum, includeHeads);
        }
        
        // next < minimum, so advance throwing next away and re-populating next with what should be >= minimum
        next();
        
        // now as long as the newly computed next exists return it and advance
        if (next != null) {
            return next();
        } else {
            includeHeads = Util.getEmpty();
            return null;
        }
    }
    
    /**
     * Advances all iterators associated with the supplied key and adds them back into the sorted multimap. If any of the sub-trees returns false, then they are
     * dropped.
     * 
     * @param key
     * @return
     */
    protected TreeMultimap<T,NestedIterator<T>> hintIterators(T key) {
        transforms.remove(key);
        for (NestedIterator<T> itr : includeHeads.removeAll(key)) {
            T hint = itr.peek();
            if (hint != null) {
                T transform = transformer.transform(hint);
                transforms.put(transform, hint);
                includeHints.put(transform, itr);
            }
        }
        
        return includeHints;
    }
    
    /**
     * Similar to <code>advanceIterators</code>, but instead of calling <code>next</code> on each sub-tree, this calls <code>move</code> with the supplied
     * <code>to</code> parameter.
     * 
     * @param key
     * @param to
     * @return
     */
    protected TreeMultimap<T,NestedIterator<T>> moveIterators(T key, T to) {
        transforms.remove(key);
        for (NestedIterator<T> itr : includeHeads.removeAll(key)) {
            T next = itr.move(to);
            if (next != null) {
                T transform = transformer.transform(next);
                transforms.put(transform, next);
                includeHeads.put(transform, itr);
            }
        }
        return includeHeads;
    }
    
    public Collection<NestedIterator<T>> leaves() {
        LinkedList<NestedIterator<T>> leaves = new LinkedList<>();
        for (NestedIterator<T> itr : includes) {
            leaves.addAll(itr.leaves());
        }
        
        // these do not include contextIncludes/contextExcludes because they will be initialized on demand
        
        return leaves;
    }
    
    public void remove() {
        throw new UnsupportedOperationException("This iterator does not support remove.");
    }
    
    public Document document() {
        return prevDocument;
    }
    
    @Override
    public Collection<NestedIterator<T>> children() {
        ArrayList<NestedIterator<T>> children = new ArrayList<>(includes.size() + contextIncludes.size() + contextExcludes.size());
        
        children.addAll(includes);
        
        children.addAll(contextIncludes);
        children.addAll(contextExcludes);
        
        return children;
    }
    
    private static <T extends Comparable<T>> TreeMultimap<T,NestedIterator<T>> initSubtree(TreeMultimap<T,NestedIterator<T>> subtree,
                    Iterable<NestedIterator<T>> sources, Util.Transformer<T> transformer, Map<T,T> transforms, boolean anded) {
        for (NestedIterator<T> src : sources) {
            src.initialize();
            T hint = src.peek();
            if (hint != null) {
                T transform = transformer.transform(hint);
                if (transforms != null) {
                    transforms.put(transform, hint);
                }
                subtree.put(transform, src);
            } else if (anded) {
                return Util.getEmpty();
            }
        }
        return subtree;
    }
    
    /**
     * Until all sourceTree keys are >= minimum AND there is something in targetTree AND nothing in sourceTree <= targetTree
     * @param sourceTree
     * @param transforms
     * @param minimum
     * @param targetTree
     */
    private void converge(TreeMultimap<T,NestedIterator<T>> sourceTree, Map<T,T> transforms, T minimum, TreeMultimap<T,NestedIterator<T>> targetTree) {
        while (sourceTree.keySet().size() > 0 && (targetTree.size() == 0 || sourceTree.keySet().first().compareTo(minimum) <= 0 || sourceTree.keySet().first().compareTo(targetTree.keySet().first()) <= 0)) {
            if (sourceTree.keySet().size() == 0) {
                return;
            }
            
            // first grab anything less than minimum (move case only)
            Set<T> moveKeys = new HashSet<>(sourceTree.keySet().headSet(minimum));
            
            // anything at minimum must be checked that it's really at minimum
            Iterator<NestedIterator<T>> nextIterator = sourceTree.removeAll(minimum).iterator();
            if (transforms != null && !targetTree.containsKey(minimum)) {
                transforms.remove(minimum);
            }
            
            while (nextIterator.hasNext()) {
                NestedIterator<T> itr = nextIterator.next();
                if (itr.hasNext()) {
                    T next = itr.next();
                    if (next != null) {
                        T transform = transformer.transform(next);
                        if (transforms != null) {
                            transforms.put(transform, next);
                        }
                        targetTree.put(transform, itr);
                    }
                }
            }
            
            // move the moveKeys
            for (T key : moveKeys) {
                Iterator<NestedIterator<T>> iterator = sourceTree.removeAll(key).iterator();
                if (transforms != null && !targetTree.containsKey(key)) {
                    transforms.remove(key);
                }
                while (iterator.hasNext()) {
                    NestedIterator<T> itr = iterator.next();
                    T next = itr.move(minimum);
                    if (next != null) {
                        T transform = transformer.transform(next);
                        if (transforms != null) {
                            transforms.put(transform, next);
                        }
                        targetTree.put(transform, itr);
                    }
                }
            }
            
            // test if the remaining sourceTree has a smallest key larger than minimum and adjust if necessary
            if (sourceTree.keySet().size() > 0 && (targetTree.size() == 0 || sourceTree.keySet().first().compareTo(targetTree.keySet().first()) <= 0)) {
                minimum = sourceTree.keySet().first();
            }
        }
        
        return;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OrIterator: ");
        
        sb.append("Includes: ");
        sb.append(includes);
        sb.append(", Deferred Includes: ");
        sb.append(contextIncludes);
        sb.append(", Deferred Excludes: ");
        sb.append(contextExcludes);
        
        return sb.toString();
    }
    
    /**
     * If there are contextIncludes or contextExcludes this iterator requires context
     * 
     * @return
     */
    @Override
    public boolean isContextRequired() {
        return !contextExcludes.isEmpty() || !contextIncludes.isEmpty();
    }
    
    /**
     * Context will be considered when evaluating contextIncludes and contextExcludes if it is lower than the lowest includes value
     * 
     * @param context
     */
    @Override
    public void setContext(T context) {
        this.evaluationContext = context;
    }
    
    /**
     * the lowest of either the includeHeads or includeHints or null if both are exhausted
     * @return
     */
    @Override
    public T peek() {
        SortedSet<T> candidates = new TreeSet<>();
        if (includeHints.keySet().size() > 0) {
            candidates.add(includeHints.keySet().first());
        }
        
        if (includeHeads.size() > 0) {
            candidates.add(includeHeads.keySet().first());
        }
        
        return candidates.size() > 0 ? candidates.first(): null;
    }
}
