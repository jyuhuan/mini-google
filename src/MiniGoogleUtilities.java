/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.collections.MultiValueHashTable;
import me.yuhuan.collections.Pair;
import me.yuhuan.net.core.ServerInfo;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by Yuhuan Jiang on 11/27/14.
 */
public class MiniGoogleUtilities {
    public static int getHashCode(String word) {
        int hashCode = 17;
        for (Character c : word.toCharArray()) {
            hashCode = hashCode * 23 + c;
        }
        return hashCode;
    }

    public synchronized static String getNextCategoryToRegisterForInTable(MultiValueHashTable<String, Pair<ServerInfo, Integer>> table) {
        int minNumHelpers = Integer.MAX_VALUE;
        String categoryThatHasMinNumHelpers = "";
        for (Map.Entry<String, Pair<Integer, ArrayList<Pair<ServerInfo, Integer>>>> entry : table) {
            int curNumHelpers = entry.getValue().item2.size();
            if (curNumHelpers < minNumHelpers) {
                minNumHelpers = curNumHelpers;
                categoryThatHasMinNumHelpers = entry.getKey();
            }
        }
        return categoryThatHasMinNumHelpers;
    }

    public synchronized static ServerInfo getMappingHelper(MultiValueHashTable<String, Pair<ServerInfo, Integer>> table) {
        for (Map.Entry<String, Pair<Integer, ArrayList<Pair<ServerInfo, Integer>>>> entry : table) {
            //ArrayList<Pair<ServerInfo, Integer>> servers = entry.getValue()
        }
        return null;
    }

}
