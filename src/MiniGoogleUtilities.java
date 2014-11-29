/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.collections.MultiValueHashTable;
import me.yuhuan.collections.Pair;
import me.yuhuan.net.core.ServerInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Yuhuan Jiang on 11/27/14.
 */
public class MiniGoogleUtilities {

    public static ArrayList<String> categories = null;

    public static int getHashCodeOf(String word) {
        int hashCode = 17;
        for (Character c : word.toCharArray()) {
            hashCode = hashCode * 23 + c;
        }
        return hashCode;
    }


    public static ArrayList<String> generateSimpleCategories() {
        if (categories != null) return categories;

        categories = new ArrayList<String>();
        categories.add("letter1part1");
        categories.add("letter1part2");
        categories.add("letter2part1");
        categories.add("letter2part2");
        categories.add("letter2part3");
        categories.add("#");
        return categories;
    }

    public static ArrayList<String> generateCategories() {
        if (categories != null) return categories;

        HashMap<String, Integer> sizes = new HashMap<String, Integer>() {};
        sizes.put("s", 56);
        sizes.put("c", 50);
        sizes.put("p", 40);
        sizes.put("b", 32);
        sizes.put("d", 31);
        sizes.put("a", 31);
        sizes.put("m", 28);
        sizes.put("t", 27);
        sizes.put("r", 26);
        sizes.put("f", 25);
        sizes.put("e", 22);
        sizes.put("i", 21);
        sizes.put("h", 21);
        sizes.put("l", 19);
        sizes.put("g", 18);
        sizes.put("w", 17);
        sizes.put("u", 15);
        sizes.put("o", 13);
        sizes.put("v", 12);
        sizes.put("n", 11);
        sizes.put("j", 5);
        sizes.put("#", 4);
        sizes.put("k", 4);
        sizes.put("q", 3);
        sizes.put("y", 2);
        sizes.put("x", 2);
        sizes.put("z", 1);

        ArrayList<String> categories = new ArrayList<String>();
        for (HashMap.Entry<String, Integer> entry : sizes.entrySet()) {
            String item = entry.getKey();
            int count = entry.getValue();
            for (int i = 0; i < count; i++) {
                categories.add(item + i);
            }
        }

        return categories;
    }

    public static String getCategoryOf(String word) {
        char firstLetter = word.charAt(0);
        if ("abcdefghijklm".indexOf(firstLetter) >= 0) {
            // falls into letter 1
            if (word.length() > 1) {
                if ("abcdef".indexOf(word.charAt(1)) >= 0) {
                    return "letter1part1";
                } else {
                    return "letter1part2";
                }
            }
            else return "letter1part1";
        }
        else if ("nopqrstuvwxyz".indexOf(firstLetter) >= 0) {
            // falls into letter 2
            if (word.length() > 1) {
                if ("nopqrst".indexOf(word.charAt(1)) >= 0) {
                    return "letter2part1";
                }
                if ("uvwxyz".indexOf(word.charAt(1)) >= 0) {
                    return "letter2part2";
                } else {
                    return "letter2part3";
                }
            }
            else return "letter2part1";

        }
        else if ("0123456789".indexOf(firstLetter) >= 0) return "#";
        else return "UNKNOWN";
    }
}
