/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

package me.yuhuan.collections;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Yuhuan Jiang on 11/27/14.
 */
public class Utilities {
    public static <T> ArrayList<T> expandByFrequency(HashMap<T, Integer> frequency) {
        ArrayList<T> result = new ArrayList<T>();
        for (HashMap.Entry<T, Integer> entry : frequency.entrySet()) {
            T item = entry.getKey();
            int count = entry.getValue();
            for (int i = 0; i < count; i++) {
                result.add(item);
            }
        }
        return result;
    }
}
