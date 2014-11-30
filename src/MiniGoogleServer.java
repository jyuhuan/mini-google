/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import com.sun.org.apache.bcel.internal.generic.NEW;
import me.yuhuan.collections.Pair;
import me.yuhuan.collections.Tuple3;
import me.yuhuan.io.Directory;
import me.yuhuan.net.Utilities;
import me.yuhuan.net.core.ServerInfo;
import me.yuhuan.net.core.TcpMessenger;
import me.yuhuan.utilities.Console;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Yuhuan Jiang on 11/24/14.
 */
public class MiniGoogleServer {

    public static class HelperMonitor implements Iterable<Map.Entry<Pair<ServerInfo, String>, Boolean>> {
        /**
         * [((Server1, pathToSeg), [Executing=False|Done=True]), ...]
         */
        volatile ConcurrentHashMap<Pair<ServerInfo, String>, Boolean> _table;

        public HelperMonitor() {
            _table = new ConcurrentHashMap<Pair<ServerInfo, String>, Boolean>();
        }

        public synchronized void addNewHelper(ServerInfo serverInfo, String pathToSeg, Boolean status) {
            Pair<ServerInfo, String> newHelper = new Pair<ServerInfo, String>(serverInfo, pathToSeg);
            if (!_table.containsKey(newHelper)) _table.put(newHelper, status);
        }

        public synchronized void addNewHelper(Pair<ServerInfo, String> newHelper, Boolean status) {
            if (!_table.containsKey(newHelper)) _table.put(newHelper, status);
        }

        public synchronized Boolean get(Pair<ServerInfo, String> helper) {
            return _table.get(helper);
        }

        public synchronized void changeStatus(ServerInfo serverInfo, String pathToSeg, Boolean status) {
            Pair<ServerInfo, String> helper = new Pair<ServerInfo, String>(serverInfo, pathToSeg);
            _table.put(helper, status);
        }

        public synchronized void removeHelper(Pair<ServerInfo, String> helper) {
            if (!_table.containsKey(helper)) _table.remove(helper);
        }

        public synchronized Boolean allHelpersDone() {
            for (Map.Entry<Pair<ServerInfo, String>, Boolean> pair : _table.entrySet()) {
                if (pair.getValue() == false) return false;
            }
            return true;
        }

        public ArrayList<Pair<ServerInfo, String>> getUnfinishedHelpers() {
            ArrayList<Pair<ServerInfo, String>> result = new ArrayList<Pair<ServerInfo, String>>();
            for (Map.Entry<Pair<ServerInfo, String>, Boolean> pair : _table.entrySet()) {
                if (pair.getValue() == false) result.add(pair.getKey());
            }
            return result;
        }


        @Override
        public Iterator<Map.Entry<Pair<ServerInfo, String>, Boolean>> iterator() {
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

        HashSet<String> _unfinishedMappingSegments;

        HelperMonitor _reducerMonitor; //TODO: change this to HashSet

        int _transactionId;

        public IndexingMaster(Socket clientSocket) {
            _requesterSocket = clientSocket;
            _unfinishedMappingSegments = new HashSet<String>();
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

        public void requestMapping(ServerInfo helper, String pathToWorkOn, String masterIpAddress, int masterPortNumber) throws IOException {
            Socket socketToCurHelper = new Socket(helper.IPAddressString(), helper.portNumber);
            TcpMessenger messengerToCurHelper = new TcpMessenger(socketToCurHelper);

            // Send request indexing mapping to helper.
            messengerToCurHelper.sendTag(Tags.REQUEST_INDEXING_MAPPING);

            // Send path to current segment to helper.
            messengerToCurHelper.sendString(pathToWorkOn);

            // Send transaction ID to helper.
            messengerToCurHelper.sendInt(_transactionId);

            // Send the master IP and Port# to helper for it to report to.
            messengerToCurHelper.sendString(masterIpAddress);
            messengerToCurHelper.sendInt(masterPortNumber);
        }

        public void run() {
            try {

                //////////////////////////////
                //         MAPPING          //
                //////////////////////////////

                Console.writeLine("Begin mapping...");

                // Create a server socket for helpers to reply status.
                ServerSocket masterServerSocket = new ServerSocket(0);
                String masterIpAddress = Utilities.getMyIpAddress();
                int masterPortNumber = Utilities.getMyPortNumber(masterServerSocket);

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

                    requestMapping(curHelper, curPath, masterIpAddress, masterPortNumber);

                    _unfinishedMappingSegments.add(curPath);

                    // masterServerSocket should be responsible for receiving the status of helpers.
                }

                // Start to collect results.

                while (_unfinishedMappingSegments.size() != 0) {
                    Console.writeLine("@@@@@@@@@@@@@@@@@@ trying...");
                    try {
                        while (_unfinishedMappingSegments.size() != 0) {
                            // Wait for helpers to respond.
                            Socket mappingHelperSocket = masterServerSocket.accept();
                            TcpMessenger mappingHelperMessenger = new TcpMessenger(mappingHelperSocket);

                            // A helper, when it finishes the mapping job, returns:
                            //    (1) The ServerInfo it is running on.
                            //    (2) The path it was working on.
                            // The helper monitor on the master uses a pair (Serverinfo, Path) to identify helpers.
                            String finishedPath = mappingHelperMessenger.receiveString();
                            _unfinishedMappingSegments.remove(finishedPath);
                        }
                    }
                    catch (SocketTimeoutException e) {
                        // This is the point where the master's server socket does not received enough responses,
                        // within the time out. This catch branch does the following:
                        //    (1) Replace each failed helper.
                        //    (2) Assign the same segment that the failed helper worked on to the new helper.

                        // Get all failed helpers.

                        // TODO: remove this. Debugging purpose only.
                        Console.writeLine("\n");
                        for (String job : _unfinishedMappingSegments) {
                            Console.writeLine(job);
                        }
                        Console.writeLine("\n");

                        ArrayList<String> failedJobs = new ArrayList<String>();
                        for (String job : _unfinishedMappingSegments) failedJobs.add(job);

                        // Borrow new helpers.
                        int numFailedHelpers = _unfinishedMappingSegments.size();
                        ArrayList<ServerInfo> newHelpers = borrowCategorylessHelpers(numFailedHelpers);

                        // Replace helpers and start jobs on new ones.
                        for (int i = 0; i < numFailedHelpers; i++) {
                            String failedJob = failedJobs.get(i);

                            // Redispatch the failed job to the new helper
                            ServerInfo newHelper = newHelpers.get(i);
                            requestMapping(newHelper, failedJob, masterIpAddress, masterPortNumber);
                        }
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
