/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.io.Directory;
import me.yuhuan.net.core.ServerInfo;
import me.yuhuan.net.core.TcpMessenger;
import me.yuhuan.utilities.Console;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Yuhuan Jiang on 11/24/14.
 */
public class MiniGoogleServer {

    public static class HelperMonitor {
        /**
         * [(Server1, [Executing=0|Done=1|Dead=-1]), ...]
         */
        volatile ConcurrentHashMap<ServerInfo, Integer> _table;

        public HelperMonitor() {
            _table = new ConcurrentHashMap<ServerInfo, Integer>();
        }

        public synchronized void addNewHelper(ServerInfo serverInfo, int status) {
            if (!_table.containsKey(serverInfo)) _table.put(serverInfo, status);
        }

        public synchronized void changeStatus(ServerInfo serverInfo, int status) {
            _table.put(serverInfo, status);
        }

        public synchronized Boolean allHelpersDone() {
            for (Map.Entry<ServerInfo, Integer> pair : _table.entrySet()) {
                if (pair.getValue() == 0) return false;
            }
            return true;
        }
    }

    // Configuration of the server
    static final String MAPPER_OUT_DIR = "working/mappers/";
    static final String REDUCER_DIR = "working/reducers/";



    public static void main(String[] args) throws IOException {

        // Creates a socket that the server listens to.
        // The port number is automatically assigned by the OS.
        ServerSocket serverSocket = new ServerSocket(0);

        // Figure out the IP address that this server is running at.
        String myIpAddress = InetAddress.getLocalHost().getHostAddress();

        // Figure out the port number that the server socket is listening to.
        int myPortNumber = serverSocket.getLocalPort();

        // Prompt to the command line the IP and port number of this server.
        Console.writeLine("I am running on " + myIpAddress + ", at port " + myPortNumber);

        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                TcpMessenger messenger = new TcpMessenger(clientSocket);

                // Determine what type of requested is received. Possible types are:
                //    (1) Indexing
                //    (2) Searching
                int tag = messenger.receiveTag();
                if (tag == Tags.REQUEST_INDEXING) {
                    (new IndexingMaster(clientSocket)).start();
                }
                else if (tag == Tags.REQUEST_SEARCHING) {
                }
            }
        }
        finally {
            serverSocket.close();
        }
    }

    static class HelperStatusMaintainer extends Thread {
        public void run() {

        }
    }

    static class IndexingMaster extends Thread {
        Socket _clientSocket;

        public IndexingMaster(Socket clientSocket) {
            _clientSocket = clientSocket;
        }

        public void run() {
            try {
                // 0. Create a messenger to the client (the one who sent the indexing request)
                TcpMessenger messengerToRequester = new TcpMessenger(_clientSocket);
                String pathToSegmentDirectory = messengerToRequester.receiveString();
                int transactionId = messengerToRequester.receiveInt();

                // 1. Create directory for the mappers to output to.
                Directory.createDirectory(MAPPER_OUT_DIR + transactionId);

                // 2. Count segments.
                ArrayList<String> pathsToSegments = Directory.getFiles(pathToSegmentDirectory);
                int numHelpersNeeded = pathsToSegments.size();;

                // 3. Contact name server, and borrow that many helpers.
                Socket socketToNameServer = new Socket("127.0.0.1", 12345);
                TcpMessenger messengerToNameServer = new TcpMessenger(socketToNameServer);
                messengerToNameServer.sendTag(Tags.REQUEST_CATEGORYLESS_HELPER);
                messengerToNameServer.sendInt(numHelpersNeeded);
                ArrayList<ServerInfo> helpers = messengerToNameServer.receiveServerInfoArray();

                // 4. Send each segment to one helper, and request mapping.
                for (int i = 0; i < numHelpersNeeded; i++) {
                    ServerInfo curHelper = helpers.get(i);
                    Socket socketToCurHelper = new Socket(curHelper.IPAddressString(), curHelper.portNumber);
                    TcpMessenger messengerToCurHelper = new TcpMessenger(socketToCurHelper);

                    // Send request indexing mapping to helper.
                    messengerToCurHelper.sendTag(Tags.REQUEST_INDEXING_MAPPING);

                    // Send path to current segment to helper.
                    messengerToCurHelper.sendString(pathsToSegments.get(i));

                    // Send transaction ID to helper.
                    messengerToCurHelper.sendInt(transactionId);

                    // Send part ID to helper.
                    messengerToCurHelper.sendInt(i);
                }



            }
            catch (IOException e) {
                Console.writeLine("IO error in indexing master. ");
            }
            finally {
                try {
                    _clientSocket.close();
                    Console.writeLine("Socket to client " + _clientSocket.getInetAddress().getHostAddress() + ":" + _clientSocket.getPort() + " is closed. ");
                }
                catch (IOException e) {
                    Console.writeLine("Socket to client " + _clientSocket.getInetAddress().getHostAddress() + ":" + _clientSocket.getPort() + " failed to close. ");
                }
            }
        }


    }
}
