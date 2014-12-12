/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import javafx.geometry.Pos;
import me.yuhuan.collections.Pair;
import me.yuhuan.io.TextFile;
import me.yuhuan.net.core.TcpMessenger;
import me.yuhuan.utilities.Console;
import me.yuhuan.utilities.UidGenerator;
import sun.util.resources.cldr.wal.CurrencyNames_wal;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Yuhuan Jiang on 12/10/14.
 */
public class MiniGoogleLib {

    static String _miniGoogleIp;
    static int _miniGooglePort;


    public static void requestIndexing(String path) throws IOException {

        String[] lines = TextFile.read("mini_google_server_info");
        _miniGoogleIp = lines[0];
        _miniGooglePort = Integer.parseInt(lines[1]);

        Socket socket = new Socket(_miniGoogleIp, _miniGooglePort);
        TcpMessenger messenger = new TcpMessenger(socket);

        messenger.sendInt(Tags.REQUEST_INDEXING);
        messenger.sendString(path);

        int transactionId = UidGenerator.next();
        messenger.sendInt(transactionId);

    }

    public static HashMap<String, ArrayList<Helper.PostingItem>> requestSearching(String[] keywords) throws IOException {
        String[] lines = TextFile.read("mini_google_server_info");
        _miniGoogleIp = lines[0];
        _miniGooglePort = Integer.parseInt(lines[1]);

        Socket socket = new Socket(_miniGoogleIp, _miniGooglePort);
        TcpMessenger messenger = new TcpMessenger(socket);
        messenger.sendTag(Tags.REQUEST_SEARCHING);
        messenger.sendInt(keywords.length);
        for (String keyword : keywords) {
            messenger.sendString(keyword);
        }

        HashMap<String, ArrayList<Helper.PostingItem>> result = new HashMap<String, ArrayList<Helper.PostingItem>>();

        for (int i = 0; i < keywords.length; i++) {
            String curKeyword = messenger.receiveString();
            ArrayList<Helper.PostingItem> curPostings = MiniGoogleUtilities.stringToPostings(messenger.receiveString());
            Collections.sort(curPostings, Collections.reverseOrder());
            result.put(curKeyword, curPostings);
        }

        return result;
    }
}
