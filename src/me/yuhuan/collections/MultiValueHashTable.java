/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

package me.yuhuan.collections;

import me.yuhuan.collections.exceptions.MultiValueHashTableLookupFailureException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Yuhuan Jiang on 11/24/14.
 */


/**
 * Represents a hash table. Each key corresponds to a list of objects.
 * When accessed by key, the value is returned in a round-Robin fashion.
 * @param <TK>
 * @param <TV>
 */
public class MultiValueHashTable<TK, TV> implements Iterable<Map.Entry<TK, Pair<Integer, ArrayList<TV>>>> {

    /**
     * Structure: [Key, [pointer, valueList]]
     */
    volatile ConcurrentHashMap<TK, Pair<Integer, ArrayList<TV>>> _table;

    public MultiValueHashTable() {
        _table = new ConcurrentHashMap<TK, Pair<Integer, ArrayList<TV>>>();
    }

    /**
     * Initializes a multi-value hash table with keys, and empty values.
     * @param keys The keys.
     */
    public MultiValueHashTable(ArrayList<TK> keys) {
        _table = new ConcurrentHashMap<TK, Pair<Integer, ArrayList<TV>>>();
        for (TK key : keys) {
            _table.put(key, new Pair<Integer, ArrayList<TV>>(0, new ArrayList<TV>()));
        }
    }

    public synchronized void add(TK key, TV value) {
        if (_table.containsKey(key)) {
            _table.get(key).item2.add(value);
        }
        else {
            ArrayList<TV> values = new ArrayList<TV>();
            values.add(value);
            _table.put(key, new Pair<Integer, ArrayList<TV> >(0, values));
        }
    }

    public synchronized TV get(TK key) throws MultiValueHashTableLookupFailureException {
        try {
            if (_table.isEmpty() || _table.size() == 0) {
                throw new MultiValueHashTableLookupFailureException("Table contains no entry. ");
            }
            else {
                int lastServerUsed = _table.get(key).item1;
                ArrayList<TV> values = _table.get(key).item2;
                lastServerUsed = (lastServerUsed + 1) % values.size();
                _table.get(key).item1 = lastServerUsed;
                return values.get(lastServerUsed);
            }
        }
        catch (NullPointerException e) {
            throw new MultiValueHashTableLookupFailureException("Table lookup error. ");
        }
    }

    public boolean containsKey(TK key) {
        return _table.containsKey(key);
    }

    @Override
    public Iterator<Map.Entry<TK, Pair<Integer, ArrayList<TV>>>> iterator() {
        return _table.entrySet().iterator();
    }


}
