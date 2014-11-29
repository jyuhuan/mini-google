/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

/*
package me.yuhuan.net.core;

import me.yuhuan.network.rpc.RpcData;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
*/
package me.yuhuan.net.core;

import me.yuhuan.net.core.ServerInfo;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * Created by Yuhuan Jiang on 10/20/14.
 */
public class TcpMessenger {

    DataOutputStream _o;
    DataInputStream _i;

    public TcpMessenger(Socket socket) throws IOException {
        _o = new DataOutputStream(socket.getOutputStream());
        _i = new DataInputStream(socket.getInputStream());
    }

    public void sendTag(int tag) throws IOException {
        _o.writeInt(tag);
    }

    public int receiveTag() throws IOException {
        return _i.readInt();
    }

    public void sendInt(int i) throws IOException {
        _o.writeInt(i);
    }

    public int receiveInt() throws IOException {
        return _i.readInt();
    }

    public void sendString(String string) throws IOException {
        _o.writeUTF(string);
    }

    public String receiveString() throws IOException {
        return _i.readUTF();
    }

    public void sendServerInfo(ServerInfo serverInfo) throws IOException {
        for (int i = 0; i < 4; i++) {
            _o.writeInt(serverInfo.ipAddress[i]);
        }
        _o.writeInt(serverInfo.portNumber);
    }

    public ServerInfo receiveServerInfo() throws IOException {
        int[] ipAddress = new int[4];
        for (int i = 0; i < 4; i++) {
            ipAddress[i] = _i.readInt();
        }
        return new ServerInfo(ipAddress, _i.readInt());
    }

    public void sendServerInfoArray(ArrayList<ServerInfo> serverInfoArray) throws IOException {
        sendInt(serverInfoArray.size());
        for (ServerInfo serverInfo : serverInfoArray) {
            sendServerInfo(serverInfo);
        }
    }

    public ArrayList<ServerInfo> receiveServerInfoArray() throws IOException {
        ArrayList<ServerInfo> result = new ArrayList<ServerInfo>();
        int arraySize = receiveInt();
        for (int i = 0; i < arraySize; i++) {
            result.add(receiveServerInfo());
        }
        return result;
    }

}
