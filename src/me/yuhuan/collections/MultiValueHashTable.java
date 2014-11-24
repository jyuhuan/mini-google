/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

package me.yuhuan.collections;

import me.yuhuan.collections.exceptions.MultiValueHashTableLookupFailureException;

import java.util.ArrayList;
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
public class MultiValueHashTable<TK, TV> {

    /**
     * Structure: [Key, [pointer, valueList]]
     */
    volatile ConcurrentHashMap<TK, Tuple2<Integer, ArrayList<TV>>> _table;

    public MultiValueHashTable() {
        _table = new ConcurrentHashMap<TK, Tuple2<Integer, ArrayList<TV>>>();
    }

    public void add(TK key, TV value) {
        if (_table.containsKey(key)) {
            _table.get(key).item2.add(value);
        }
        else {
            ArrayList<TV> values = new ArrayList<TV>();
            values.add(value);
            _table.put(key, new Tuple2<Integer, ArrayList<TV> >(0, values));
        }
    }

    public TV get(TK key) throws MultiValueHashTableLookupFailureException {
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


}
