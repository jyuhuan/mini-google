/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.net.core.TcpMessenger;
import me.yuhuan.utilities.UidGenerator;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * Created by Yuhuan Jiang on 12/10/14.
 */
public class MiniGoogleLib {

    static String _miniGoogleIp;
    static int _miniGooglePort;

    public static void requestIndexing(String path) throws IOException {
        _miniGoogleIp = "127.0.0.1";
        _miniGooglePort = 5555;

        Socket socket = new Socket(_miniGoogleIp, _miniGooglePort);
        TcpMessenger messenger = new TcpMessenger(socket);

        messenger.sendInt(Tags.REQUEST_INDEXING);
        messenger.sendString(path);
        messenger.sendInt(UidGenerator.next());
    }

    public static void requestSearching(String[] keywords) throws IOException {
        _miniGoogleIp = "127.0.0.1";
        _miniGooglePort = 5555;

        Socket socket = new Socket(_miniGoogleIp, _miniGooglePort);
        TcpMessenger messenger = new TcpMessenger(socket);
        messenger.sendTag(Tags.REQUEST_SEARCHING);
        messenger.sendInt(keywords.length);
        for (String keyword : keywords) {
            messenger.sendString(keyword);
        }
    }
}
