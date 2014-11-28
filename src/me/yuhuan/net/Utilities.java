/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

package me.yuhuan.net;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

/**
 * Created by Yuhuan Jiang on 11/27/14.
 */
public class Utilities {
    public static String getMyIpAddress() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostAddress();
    }

    public static int getMyPortNumber(ServerSocket serverSocket) {
        return serverSocket.getLocalPort();
    }
}
