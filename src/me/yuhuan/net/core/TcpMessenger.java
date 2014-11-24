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

/**
 * Created by Yuhuan Jiang on 10/20/14.
 */
public class TcpMessenger {
/*
    public static void sendTag(DataOutputStream dataOutputStream, int tag) throws IOException {
        dataOutputStream.writeInt(tag);
    }

    public static int receiveTag(DataInputStream dataInputStream) throws IOException {
        return dataInputStream.readInt();
    }

    public static void sendPortNumber(DataOutputStream dataOutputStream, int portNumber) throws IOException {
        dataOutputStream.writeInt(portNumber);
    }

    public static int receivePortNumber(DataInputStream dataInputStream) throws IOException {
        return dataInputStream.readInt();
    }

    public static void sendTransactionId(DataOutputStream dataOutputStream, int transactionId) throws IOException {
        dataOutputStream.writeInt(transactionId);
    }

    public static int receiveTransactionId(DataInputStream dataInputStream) throws IOException {
        return dataInputStream.readInt();
    }

    public static void sendProcedureInfo(DataOutputStream dataOutputStream, ProcedureInfo procedureInfo) throws IOException {
        dataOutputStream.writeInt(procedureInfo.programID);
        dataOutputStream.writeInt(procedureInfo.procedureID);
        dataOutputStream.writeInt(procedureInfo.versionID);
    }

    public static ProcedureInfo receiveProcedureInfo(DataInputStream dataInputStream) throws IOException {
        return new ProcedureInfo(dataInputStream.readInt(), dataInputStream.readInt(), dataInputStream.readInt());
    }

    public static void sendProcedureInfos(DataOutputStream dataOutputStream, ArrayList<ProcedureInfo> procedureInfos) throws IOException {
        int numberOfProcedureInfos = procedureInfos.size();
        dataOutputStream.writeInt(numberOfProcedureInfos);
        for (ProcedureInfo p: procedureInfos) {
            dataOutputStream.writeInt(p.programID);
            dataOutputStream.writeInt(p.procedureID);
            dataOutputStream.writeInt(p.versionID);
        }
    }

    public static ArrayList<ProcedureInfo> receiveProcedureInfos(DataInputStream dataInputStream) throws IOException {
        ArrayList<ProcedureInfo> result = new ArrayList<ProcedureInfo>();
        int numberOfProcedureInfos = dataInputStream.readInt();
        for (int i = 0; i < numberOfProcedureInfos; i++) {
            result.add(new ProcedureInfo(dataInputStream.readInt(), dataInputStream.readInt(), dataInputStream.readInt()));
        }
        return result;
    }

    public static void sendServerInfo(DataOutputStream dataOutputStream, ServerInfo serverInfo) throws IOException {
        for (int i = 0; i < 4; i++) {
            dataOutputStream.writeInt(serverInfo.ipAddress[i]);
        }
        dataOutputStream.writeInt(serverInfo.portNumber);
    }

    public static ServerInfo receiveServerInfo(DataInputStream dataInputStream) throws IOException {
        int[] ipAddress = new int[4];
        for (int i = 0; i < 4; i++) {
            ipAddress[i] = dataInputStream.readInt();
        }
        return new ServerInfo(ipAddress, dataInputStream.readInt());
    }

    public static void sendRpcData(DataOutputStream dataOutputStream, RpcData data) throws IOException {
        dataOutputStream.write(data.toBytes());
    }

    public static RpcData receiveRpcData(DataInputStream dataInputStream) throws IOException {
        byte[] bytes = new byte[1000];
        dataInputStream.read(bytes);
        return new RpcData(bytes);
    }*/
}
