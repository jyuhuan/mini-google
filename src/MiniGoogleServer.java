/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.collections.Pair;
import me.yuhuan.collections.Tuple3;
import me.yuhuan.io.Directory;
import me.yuhuan.net.core.ServerInfo;
import me.yuhuan.net.core.TcpMessenger;
import me.yuhuan.utilities.Console;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Yuhuan Jiang on 11/24/14.
 */
public class MiniGoogleServer {

    public static class HelperMonitor implements Iterable<Map.Entry<ServerInfo, Tuple3<Integer, String, Boolean>>> {
        /**
         * [(Server1, [Executing=False|Done=True]), ...]
         */
        volatile ConcurrentHashMap<ServerInfo, Tuple3<Integer, String, Boolean>> _table;

        public HelperMonitor() {
            _table = new ConcurrentHashMap<ServerInfo, Tuple3<Integer, String, Boolean>>();
        }

        public synchronized void addNewHelper(ServerInfo serverInfo, int partId, String pathToSeg, Boolean status) {
            if (!_table.containsKey(serverInfo)) _table.put(serverInfo, new Tuple3<Integer, String, Boolean>(partId, pathToSeg, status));
        }

        public synchronized Tuple3<Integer, String, Boolean> get(ServerInfo helper) {
            return _table.get(helper);
        }

        public synchronized void changeStatus(ServerInfo serverInfo, Boolean status) {
            _table.get(serverInfo).item3 = status;
        }

        public synchronized void removeHelper(ServerInfo serverInfo) {
            if (!_table.containsKey(serverInfo)) _table.remove(serverInfo);
        }

        public synchronized Boolean allHelpersDone() {
            for (Map.Entry<ServerInfo, Tuple3<Integer, String, Boolean>> pair : _table.entrySet()) {
                if (pair.getValue().item3 == false) return false;
            }
            return true;
        }

        public ArrayList<ServerInfo> getUnfinishedHelpers() {
            ArrayList<ServerInfo> result = new ArrayList<ServerInfo>();
            for (Map.Entry<ServerInfo, Tuple3<Integer, String, Boolean>> pair : _table.entrySet()) {
                if (pair.getValue().item3 == false) result.add(pair.getKey());
            }
            return result;
        }


        @Override
        public Iterator<Map.Entry<ServerInfo, Tuple3<Integer, String, Boolean>>> iterator() {
            return _table.entrySet().iterator();
        }
    }

    // Configuration of the server
    static final int PORT = 5555;

    static final String MAPPER_OUT_DIR = "working/mappers/";
    static final String REDUCER_DIR = "working/reducers/";

    static final int MAX_WAIT_TIME_FOR_HELPER = 10000;


    public static void main(String[] args) throws IOException {

        // Creates a socket that the server listens to.
        // The port number is automatically assigned by the OS.
        ServerSocket serverSocket = new ServerSocket(PORT);

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

    static class IndexingMaster extends Thread {
        Socket _requesterSocket;
        HelperMonitor _mapperMonitor;
        HelperMonitor _reducerMonitor;

        int _transactionId;

        public IndexingMaster(Socket clientSocket) {
            _requesterSocket = clientSocket;
            _mapperMonitor = new HelperMonitor();
            _reducerMonitor = new HelperMonitor();
        }

        public ArrayList<ServerInfo> borrowCategorylessHelpers(int numHelpersNeeded) throws IOException {
            if (numHelpersNeeded == 0) return new ArrayList<ServerInfo>();

            // Contact name server, and borrow that many helpers.
            Socket socketToNameServer = new Socket("127.0.0.1", 12345); // TODO: change this hardcoded IP and Port# to file reading.
            TcpMessenger messengerToNameServer = new TcpMessenger(socketToNameServer);
            messengerToNameServer.sendTag(Tags.REQUEST_CATEGORYLESS_HELPER);
            messengerToNameServer.sendInt(numHelpersNeeded);
            ArrayList<ServerInfo> helpers = messengerToNameServer.receiveServerInfoArray();
            return helpers;
        }

        public void requestMapping(ServerInfo helper, String pathToWorkOn, int partId) throws IOException {
            Socket socketToCurHelper = new Socket(helper.IPAddressString(), helper.portNumber);
            TcpMessenger messengerToCurHelper = new TcpMessenger(socketToCurHelper);

            // Send request indexing mapping to helper.
            messengerToCurHelper.sendTag(Tags.REQUEST_INDEXING_MAPPING);

            // Send path to current segment to helper.
            messengerToCurHelper.sendString(pathToWorkOn);

            // Send transaction ID to helper.
            messengerToCurHelper.sendInt(_transactionId);

            // Send part ID to helper.
            messengerToCurHelper.sendInt(partId);
        }

        public void run() {
            try {

                //////////////////////////////
                //         MAPPING          //
                //////////////////////////////

                Console.writeLine("Begin mapping...");

                // Create a server socket for helpers to reply status.
                ServerSocket masterServerSocket = new ServerSocket(0);
                masterServerSocket.setSoTimeout(MAX_WAIT_TIME_FOR_HELPER);

                // Create a messenger to the one who sent the indexing request
                TcpMessenger messengerToRequester = new TcpMessenger(_requesterSocket);

                // Receive parameters.
                // First parameter: path to the directory of segments.
                String pathToSegmentDirectory = messengerToRequester.receiveString();

                // Second parameter: transaction ID. This is used to create the directory of partial results.
                _transactionId = messengerToRequester.receiveInt();

                // Create directory for the mappers to output the partial result to.
                Directory.createDirectory(MAPPER_OUT_DIR + _transactionId);

                // Count segments.
                ArrayList<String> pathsToSegments = Directory.getFiles(pathToSegmentDirectory);
                int numHelpersNeeded = pathsToSegments.size();

                // Contact name server, and borrow that many helpers.
                ArrayList<ServerInfo> helpers = borrowCategorylessHelpers(numHelpersNeeded);

                // Send each segment to one helper, and request mapping.
                for (int i = 0; i < numHelpersNeeded; i++) {
                    ServerInfo curHelper = helpers.get(i);
                    String curPath = pathsToSegments.get(i);

                    requestMapping(curHelper, curPath, i);

                    // From this point on, the helper is executing the mapping.
                    _mapperMonitor.addNewHelper(curHelper, i, curPath, false);

                    // masterServerSocket should be responsible for receiving the status of helpers.
                }

                // Start to collect results.

                ArrayList<ServerInfo> unfinished = _mapperMonitor.getUnfinishedHelpers();
                while (unfinished.size() != 0) {
                    Console.writeLine("@@@@@@@@@@@@@@@@@@ trying...");
                    try {
                        while (!_mapperMonitor.allHelpersDone()) {
                            Socket mappingHelperSocket = masterServerSocket.accept();
                            TcpMessenger mappingHelperMessenger = new TcpMessenger(mappingHelperSocket);
                            ServerInfo mappingHelper = mappingHelperMessenger.receiveServerInfo();
                            _mapperMonitor.changeStatus(mappingHelper, true);
                        }
                    }
                    catch (SocketTimeoutException e) {
                        ArrayList<ServerInfo> failedHelpers = _mapperMonitor.getUnfinishedHelpers();

                        Console.writeLine("\n" + failedHelpers.toString() + "\n");

                        int numFailedHelpers = failedHelpers.size();
                        ArrayList<ServerInfo> newHelpers = borrowCategorylessHelpers(numFailedHelpers);

                        for (int i = 0; i < numFailedHelpers; i++) {
                            ServerInfo failedHelper = failedHelpers.get(i);

                            // Get partId and path that the failed helper used to work on
                            Tuple3<Integer, String, Boolean> helperInformation = _mapperMonitor.get(failedHelper);
                            int partIdThatTheFailedHelperUsedToWorkOn = helperInformation.item1;
                            String pathThatTheFailedHelperUsedToWorkOn = helperInformation.item2;

                            // Redispatch the failed job to the new helper
                            ServerInfo newHelper = newHelpers.get(i);
                            requestMapping(newHelper, pathThatTheFailedHelperUsedToWorkOn, partIdThatTheFailedHelperUsedToWorkOn);

                            // Remove the failed helper from the monitor
                            _mapperMonitor.removeHelper(failedHelper);
                        }
                        unfinished = _mapperMonitor.getUnfinishedHelpers();
                    }
                }

                Console.writeLine("Mapping done... \n");

                //////////////////////////////
                //         REDUCING         //
                //////////////////////////////



            }
            catch (IOException e) {
                Console.writeLine("IO error in indexing master. ");
            }
            finally {
                try {
                    _requesterSocket.close();
                    Console.writeLine("Socket to client " + _requesterSocket.getInetAddress().getHostAddress() + ":" + _requesterSocket.getPort() + " is closed. ");
                }
                catch (IOException e) {
                    Console.writeLine("Socket to client " + _requesterSocket.getInetAddress().getHostAddress() + ":" + _requesterSocket.getPort() + " failed to close. ");
                }
            }
        }


    }
}
