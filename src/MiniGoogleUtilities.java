/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.collections.MultiValueHashTable;
import me.yuhuan.collections.Pair;
import me.yuhuan.net.core.ServerInfo;
import me.yuhuan.net.core.TcpMessenger;

import java.io.IOException;
import java.net.Socket;
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
        categories.add("a");
        categories.add("b");
        categories.add("c");
        categories.add("d");
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
        if (firstLetter == 'a') return "a";
        else if (firstLetter == 'b') return "b";
        else if (firstLetter == 'c') return "c";
        else if (firstLetter == 'd') return "d";
        else if ("0123456789".indexOf(firstLetter) >= 0) return "#";
        else return "UNK";
    }

    public static ArrayList<ServerInfo> borrowCategorylessHelpers(int numHelpersNeeded) throws IOException {
        if (numHelpersNeeded == 0) return new ArrayList<ServerInfo>();

        // Contact name server, and borrow that many helpers.
        Socket socketToNameServer = new Socket("127.0.0.1", 12345); // TODO: change this hardcoded IP and Port# to file reading.
        TcpMessenger messengerToNameServer = new TcpMessenger(socketToNameServer);
        messengerToNameServer.sendTag(Tags.REQUEST_CATEGORYLESS_HELPER);
        messengerToNameServer.sendInt(numHelpersNeeded);
        return messengerToNameServer.receiveServerInfoArray();
    }

    public static ServerInfo borrowOneCategoriedHelper(String category) throws IOException {
        // Contact name server, and borrow that many helpers.
        Socket socketToNameServer = new Socket("127.0.0.1", 12345); // TODO: change this hardcoded IP and Port# to file reading.
        TcpMessenger messengerToNameServer = new TcpMessenger(socketToNameServer);
        messengerToNameServer.sendTag(Tags.REQUEST_CATEGORY_HELPER);
        messengerToNameServer.sendString(category);
        return messengerToNameServer.receiveServerInfo();
    }

    public static ArrayList<ServerInfo> borrowASetOfReducingHelpers() throws IOException {
        // Contact name server, and borrow that many helpers.
        Socket socketToNameServer = new Socket("127.0.0.1", 12345); // TODO: change this hardcoded IP and Port# to file reading.
        TcpMessenger messengerToNameServer = new TcpMessenger(socketToNameServer);
        messengerToNameServer.sendTag(Tags.REQUEST_A_SET_OF_CATEGORY_HELPER);
        return messengerToNameServer.receiveServerInfoArray();
    }


}
