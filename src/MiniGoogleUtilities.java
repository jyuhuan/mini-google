/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.collections.Pair;
import me.yuhuan.net.core.ServerInfo;
import me.yuhuan.net.core.TcpMessenger;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.regex.Pattern;

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

    public static boolean isWord(String word) {
        if (word.length() < 1) return false;

        // TODO: stop words

        for (Character c : word.toCharArray()) {
            if (!contains("abcdefghijklmnopqrstuvwxyz0123456789", c)) return false;
        }
        return true;
    }

    public static String postingsToString(ArrayList<Helper.PostingItem> postings) {
        if (postings == null || postings.size() == 0) return "";
        StringBuilder builder = new StringBuilder();
        for (Helper.PostingItem posting : postings) {
            builder.append("|");
            builder.append(posting.getDocumentName());
            builder.append(",");
            builder.append(posting.getFrequency());
        }
        return builder.substring(1);
    }

    public static ArrayList<Helper.PostingItem> stringToPostings(String s) {
        if (s.equals("")) return new ArrayList<Helper.PostingItem>();
        ArrayList<Helper.PostingItem> postings = new ArrayList<Helper.PostingItem>();
        String[] stringsOfPairs = s.split("\\|");
        for (String stringOfPair : stringsOfPairs) {
            String[] itemStrings = stringOfPair.split(",");
            postings.add(new Helper.PostingItem(itemStrings[0], Integer.parseInt(itemStrings[1])));
        }
        return postings;
    }

    public static String getDirectoryName(String pathToDirectory) {
        String[] parts = pathToDirectory.split(Pattern.quote(File.separator));
        return parts[parts.length - 1];
    }

    /*public static ArrayList<String> generateCategories() {
        if (categories != null) return categories;
        categories = new ArrayList<String>();
        categories.add("c1");
        categories.add("c2");
        categories.add("c3");
        categories.add("c4");
        categories.add("#");
        return categories;
    }

        public static String getCategoryOf(String word) {
        char firstLetter = word.charAt(0);

        if (contains("scp", firstLetter)) return "c1";
        else if (contains("bdamt", firstLetter)) return "c2";
        else if (contains("rfeihlg", firstLetter)) return "c3";
        else if (contains("wuovnjkqyxz", firstLetter)) return "c4";
        else if (contains("0123456789", firstLetter)) return "#";
        else return "UNK";
    }*/


    public static boolean contains(String s, char c) {
        return s.indexOf(c) >= 0;
    }


    public static ArrayList<String> generateCategories() {
        if (categories != null) return categories;

        categories = new ArrayList<String>();
        for (Character character : "abcdefghijklmnopqrstuvwxyz#".toCharArray()) {
            categories.add(String.valueOf(character));
        }
        return categories;
    }

    public static String getCategoryOf(String word) {
        try {
            char firstLetter = word.charAt(0);
            if ("abcdefghijklmnopqrstuvwxyz".indexOf(firstLetter) >= 0) return String.valueOf(firstLetter);
            else if ("0123456789".indexOf(firstLetter) >= 0) return "#";
            else return "UNK";
        }
        catch (Exception e) {
            return "UNK";
        }
    }


    public static ArrayList<ServerInfo> borrowCategorylessHelpers(int numHelpersNeeded, ServerInfo nameServerInfo) throws IOException {
        if (numHelpersNeeded == 0) return new ArrayList<ServerInfo>();

        // Contact name server, and borrow that many helpers.
        try {
            Socket socketToNameServer = new Socket(nameServerInfo.IPAddressString(), nameServerInfo.portNumber);
            TcpMessenger messengerToNameServer = new TcpMessenger(socketToNameServer);
            messengerToNameServer.sendTag(Tags.REQUEST_CATEGORYLESS_HELPER);
            messengerToNameServer.sendInt(numHelpersNeeded);
            ArrayList<ServerInfo> result = messengerToNameServer.receiveServerInfoArray();
            socketToNameServer.close();
            return result;
        }
        catch (IOException e) {
            return new ArrayList<ServerInfo>();
        }
    }

    public static ServerInfo borrowOneCategoriedHelper(String category, ServerInfo nameServerInfo) throws IOException {
        // Contact name server, and borrow that many helpers.
        try {
            Socket socketToNameServer = new Socket(nameServerInfo.IPAddressString(), nameServerInfo.portNumber);
            TcpMessenger messengerToNameServer = new TcpMessenger(socketToNameServer);
            messengerToNameServer.sendTag(Tags.REQUEST_CATEGORY_HELPER);
            messengerToNameServer.sendString(category);
            ServerInfo result = messengerToNameServer.receiveServerInfo();
            socketToNameServer.close();
            return result;
        }
        catch (IOException e) {
            return ServerInfo.createFakeServer();
        }
    }

    public static ArrayList<ServerInfo> borrowASetOfReducingHelpers(ServerInfo nameServerInfo) throws IOException {
        // Contact name server, and borrow that many helpers.
        try {
            Socket socketToNameServer = new Socket(nameServerInfo.IPAddressString(), nameServerInfo.portNumber);
            TcpMessenger messengerToNameServer = new TcpMessenger(socketToNameServer);
            messengerToNameServer.sendTag(Tags.REQUEST_A_SET_OF_CATEGORY_HELPER);
            ArrayList<ServerInfo> result = messengerToNameServer.receiveServerInfoArray();
            socketToNameServer.close();
            return result;
        } catch (IOException e) {
            return new ArrayList<ServerInfo>();
        }
    }


}
