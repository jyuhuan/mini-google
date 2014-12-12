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
import java.util.*;

/**
 * Created by Yuhuan Jiang on 12/10/14.
 */
public class MiniGoogleLib {

    static String _miniGoogleIp;
    static int _miniGooglePort;

    public static class ResultList {
        HashMap<String, Pair<Long, ArrayList<Helper.PostingItem>>> _results;

        public ResultList() {
            _results = new HashMap<String, Pair<Long, ArrayList<Helper.PostingItem>>>();
        }

        static final long EXPIRATION_LIMIT = 10000;

        private long getCurrentTime() {
            Date date = new Date();
            return date.getTime();
        }

        public void add(String word, ArrayList<Helper.PostingItem> postings) {
            _results.put(word, new Pair<Long, ArrayList<Helper.PostingItem>>(getCurrentTime(), postings));
        }

        public ArrayList<Helper.PostingItem> get(String word) {
            if (!_results.containsKey(word)) return null;
            long curTime = getCurrentTime();
            Pair<Long, ArrayList<Helper.PostingItem>> entry = _results.get(word);
            long oldTime = entry.item1;
            if (curTime - oldTime < EXPIRATION_LIMIT) return entry.item2;
            return null;
        }
    }

    static ResultList _cacheResults = new ResultList();

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
        HashMap<String, ArrayList<Helper.PostingItem>> result = new HashMap<String, ArrayList<Helper.PostingItem>>();

        // Look for cached results
        ArrayList<String> unfinishedQuery = new ArrayList<String>();
        for (String keyword : keywords) {
            ArrayList<Helper.PostingItem> cachedPostings = _cacheResults.get(keyword);
            if (cachedPostings != null) {
                result.put(keyword, cachedPostings);
            }
            else {
                unfinishedQuery.add(keyword);
            }
        }

        // if some result for some keywords are not in the cache, do actual search query.

        if (unfinishedQuery.size() != 0) {
            String[] lines = TextFile.read("mini_google_server_info");
            _miniGoogleIp = lines[0];
            _miniGooglePort = Integer.parseInt(lines[1]);

            Socket socket = new Socket(_miniGoogleIp, _miniGooglePort);
            TcpMessenger messenger = new TcpMessenger(socket);
            messenger.sendTag(Tags.REQUEST_SEARCHING);
            messenger.sendInt(keywords.length);
            for (String keyword : unfinishedQuery) {
                messenger.sendString(keyword);
            }

            for (int i = 0; i < keywords.length; i++) {
                String curKeyword = messenger.receiveString();
                ArrayList<Helper.PostingItem> curPostings = MiniGoogleUtilities.stringToPostings(messenger.receiveString());
                Collections.sort(curPostings, Collections.reverseOrder());
                result.put(curKeyword, curPostings);
                _cacheResults.add(curKeyword, curPostings);
            }

        }

        return result;
    }
}
