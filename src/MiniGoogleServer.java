/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.io.Directory;
import me.yuhuan.io.TextFile;
import me.yuhuan.net.Utilities;
import me.yuhuan.net.core.ServerInfo;
import me.yuhuan.net.core.TcpMessenger;
import me.yuhuan.utilities.Console;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;

/**
 * Created by Yuhuan Jiang on 11/24/14.
 */

/**
 * Represents a MiniGoogle server that takes in either
 */
public class MiniGoogleServer {

    // Configuration of the server

    /**
     * The port number that the server socket listens to.
     */
    static final int PORT = 0;

    /**
     * The directory that all mappers should output to. Structure of this directory:
     * ./working/mappers/transactionId1/
     */
    static final String MAPPER_OUT_DIR = "working/mappers/";
    static final int MAX_WAIT_TIME_FOR_HELPER = 10000;
    static final int MAX_TRIAL_NUM = 3;

    static ServerInfo _nameServerInfo;

    public static void main(String[] args) throws IOException {

        String[] lines = TextFile.read("name_server_info");
        String nameServerIpAddress = lines[0];
        int nameServerPortNumber = Integer.parseInt(lines[1]);
        _nameServerInfo = new ServerInfo(nameServerIpAddress, nameServerPortNumber);

        // Creates a socket that the server listens to.
        // The port number is automatically assigned by the OS.
        ServerSocket serverSocket = new ServerSocket(PORT);

        // Figure out the IP address that this server is running at.
        String myIpAddress = InetAddress.getLocalHost().getHostAddress();

        // Figure out the port number that the server socket is listening to.
        int myPortNumber = serverSocket.getLocalPort();

        // Prompt to the command line the IP and port number of this server.
        Console.writeLine("I am running on " + myIpAddress + ", at port " + myPortNumber);

        TextFile.write("mini_google_server_info", new String[]{ myIpAddress, String.valueOf(myPortNumber) });


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
                    (new SearchingMaster(clientSocket)).start();
                }
            }
        }
        finally {
            serverSocket.close();
        }
    }

    /**
     * A thread that coordinates an indexing request.
     * Note: use a new instance of this thread for each transaction.
     */
    static class IndexingMaster extends Thread {
        /**
         * A socket to the requester, usually the client that requested the indexing transaction.
         */
        Socket _requesterSocket;

        /**
         * A set of unfinished mapping jobs. Each item is a segment path, representing a job.
         */
        HashSet<String> _unfinishedMappingJobs;

        /**
         * A set of unfinished reducing jobs. Each item is a segment path, representing a job.
         */
        HashSet<String> _unfinishedReducingJobs;

        int _transactionId;

        public IndexingMaster(Socket clientSocket) {
            _requesterSocket = clientSocket;
            _unfinishedMappingJobs = new HashSet<String>();
            _unfinishedReducingJobs = new HashSet<String>();
        }

        public void requestMapping(ServerInfo helper, String pathToWorkOn, String masterIpAddress, int masterPortNumber) throws IOException {
            try {
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

                socketToCurHelper.close();
            }
            catch (IOException e) { }
        }

        public void requestReducing(ServerInfo helper, String documentName, String masterIpAddress, int masterPortNumber) throws IOException {
            try {
                Socket socketToHelper = new Socket(helper.IPAddressString(), helper.portNumber);
                TcpMessenger messenger = new TcpMessenger(socketToHelper);

                messenger.sendTag(Tags.REQUEST_INDEXING_REDUCING);

                // Send transaction ID to helper. The helper will use it to locate the partial count directory.
                messenger.sendInt(_transactionId);

                // Send the document name so that the reducer can create the postings.
                messenger.sendString(documentName);

                // Send the master IP and Port# to helper for it to report to.
                messenger.sendString(masterIpAddress);
                messenger.sendInt(masterPortNumber);

                socketToHelper.close();
            }
            catch (IOException e) { }
        }

        public void run() {
            try {


                String masterIpAddress = Utilities.getMyIpAddress();


                //////////////////////////////
                //         MAPPING          //
                //////////////////////////////

                Console.writeLine("Begin mapping...");

                // Create a server socket for mapping helpers to reply status.
                ServerSocket masterServerSocketForMapping = new ServerSocket(0, 200);
                int masterPortNumberForMapping = Utilities.getMyPortNumber(masterServerSocketForMapping);

                masterServerSocketForMapping.setSoTimeout(MAX_WAIT_TIME_FOR_HELPER);

                // Create a messenger to the one who sent the indexing request
                TcpMessenger messengerToRequester = new TcpMessenger(_requesterSocket);

                // Receive parameters.
                // First parameter: path to the directory of segments.
                String pathToSegmentDirectory = messengerToRequester.receiveString();

                String documentName = MiniGoogleUtilities.getDirectoryName(pathToSegmentDirectory);

                // Second parameter: transaction ID. This is used to create the directory of partial results.
                _transactionId = messengerToRequester.receiveInt();

                // Create directory for the mappers to output the partial result to.
                Directory.createDirectory(MAPPER_OUT_DIR + _transactionId);

                // Count segments.
                ArrayList<String> pathsToSegments = Directory.getFiles(pathToSegmentDirectory);
                int numHelpersNeeded = pathsToSegments.size();

                // Contact name server, and borrow that many helpers.
                ArrayList<ServerInfo> helpers = MiniGoogleUtilities.borrowCategorylessHelpers(numHelpersNeeded, _nameServerInfo);

                // Send each segment to one helper, and request mapping.
                for (int i = 0; i < numHelpersNeeded; i++) {
                    ServerInfo curHelper = helpers.get(i);
                    String curPath = pathsToSegments.get(i);

                    requestMapping(curHelper, curPath, masterIpAddress, masterPortNumberForMapping);

                    _unfinishedMappingJobs.add(curPath);

                    // masterServerSocket should be responsible for receiving the status of helpers.
                }

                // Start to collect results.
                int numTrialsForMapping = 0;
                while (_unfinishedMappingJobs.size() != 0 && numTrialsForMapping < MAX_TRIAL_NUM) {
                    Console.writeLine("@@@@@@@@@@@@@@@@@@ trying...");
                    try {
                        while (_unfinishedMappingJobs.size() != 0) {
                            // Wait for helpers to respond.
                            Socket mappingHelperSocket = masterServerSocketForMapping.accept();
                            TcpMessenger mappingHelperMessenger = new TcpMessenger(mappingHelperSocket);

                            // A helper, when it finishes the mapping job, returns:
                            //    (1) The ServerInfo it is running on.
                            //    (2) The path it was working on.
                            // The helper monitor on the master uses a pair (Serverinfo, Path) to identify helpers.
                            String finishedPath = mappingHelperMessenger.receiveString();
                            mappingHelperSocket.close();
                            _unfinishedMappingJobs.remove(finishedPath);
                        }
                    }
                    catch (Exception e) {
                        // This is the point where the master's server socket does not received enough responses,
                        // within the time out. This catch branch does the following:
                        //    (1) Replace each failed helper.
                        //    (2) Assign the same segment that the failed helper worked on to the new helper.

                        // Get all failed helpers.

                        numTrialsForMapping++;
                        Console.writeLine("\n");
                        for (String job : _unfinishedMappingJobs) {
                            Console.writeLine(job);
                        }
                        Console.writeLine("\n");

                        ArrayList<String> failedJobs = new ArrayList<String>();
                        for (String job : _unfinishedMappingJobs) failedJobs.add(job);

                        // Borrow new helpers.
                        int numFailedHelpers = _unfinishedMappingJobs.size();
                        ArrayList<ServerInfo> newHelpers = MiniGoogleUtilities.borrowCategorylessHelpers(numFailedHelpers, _nameServerInfo);

                        // Replace helpers and start jobs on new ones.
                        for (int i = 0; i < numFailedHelpers; i++) {
                            String failedJob = failedJobs.get(i);

                            // Redispatch the failed job to the new helper
                            ServerInfo newHelper = newHelpers.get(i);
                            requestMapping(newHelper, failedJob, masterIpAddress, masterPortNumberForMapping);
                        }
                    }
                }

                if (numTrialsForMapping == MAX_TRIAL_NUM) {
                    Console.writeLine("Give up.");
                    throw new IOException("Mapping failed. ");
                }

                masterServerSocketForMapping.close();
                Console.writeLine("Mapping done... \n");

                //////////////////////////////
                //         REDUCING         //
                //////////////////////////////

                Console.writeLine("Starting reducing...");

                // Create a server socket for reducing helpers to reply status.
                ServerSocket masterServerSocketForReducing = new ServerSocket(0);
                int masterPortNumberForReducing = Utilities.getMyPortNumber(masterServerSocketForReducing);
                masterServerSocketForReducing.setSoTimeout(MAX_WAIT_TIME_FOR_HELPER);

                ArrayList<ServerInfo> reducingHelpers = MiniGoogleUtilities.borrowASetOfReducingHelpers(_nameServerInfo);

                for (ServerInfo serverInfo : reducingHelpers) {
                    requestReducing(serverInfo, documentName, masterIpAddress, masterPortNumberForReducing);
                }

                for (String category : MiniGoogleUtilities.generateCategories()) {
                    _unfinishedReducingJobs.add(category);
                }


                // Start to collect results.
                int numTrialsForReducing = 0;
                while (_unfinishedReducingJobs.size() != 0 && numTrialsForReducing < MAX_TRIAL_NUM) {
                    Console.writeLine("@@@@@@@@@@@@@@@@@@ trying...");
                    try {
                        while (_unfinishedReducingJobs.size() != 0) {
                            // Wait for helpers to respond.
                            Socket reducingHelperSocket = masterServerSocketForReducing.accept();
                            TcpMessenger reducingHelperMessenger = new TcpMessenger(reducingHelperSocket);

                            // A helper, when it finishes the reducing job, returns:
                            //    (1) The ServerInfo it is running on.
                            //    (2) The category it was working on.
                            // The helper monitor on the master uses a pair (Serverinfo, Path) to identify helpers.
                            String finishedCategory = reducingHelperMessenger.receiveString();
                            reducingHelperSocket.close();
                            _unfinishedReducingJobs.remove(finishedCategory);
                        }
                    }
                    catch (Exception e) {
                        numTrialsForReducing++;
                        Console.writeLine("\n");
                        for (String job : _unfinishedReducingJobs) {
                            Console.writeLine(job);
                        }
                        Console.writeLine("\n");

                        for (String failedReducingJob : _unfinishedReducingJobs) {
                            ServerInfo newReducingHelper = MiniGoogleUtilities.borrowOneCategoriedHelper(failedReducingJob, _nameServerInfo);
                            if (!newReducingHelper.equals(ServerInfo.createFakeServer())) {
                                requestReducing(newReducingHelper, documentName, masterIpAddress, masterPortNumberForReducing);
                            }
                        }
                    }
                }

                if (numTrialsForReducing == MAX_TRIAL_NUM) {
                    Console.writeLine("Give up");
                    throw new IOException("Reducing failed. ");
                }



                // delete all intermediate results in this transaction
                Directory.removeDirectory(MAPPER_OUT_DIR + _transactionId);

                masterServerSocketForReducing.close();
                Console.writeLine("Reducing done... \n");

                TcpMessenger messenger = new TcpMessenger(_requesterSocket);
                messenger.sendTag(Tags.MESSAGE_INDEXING_DONE);

            }
            catch (IOException e) {
                Console.writeLine("IO error in indexing master. ");
                Console.writeLine("\t" + e.getMessage());
                e.printStackTrace();
                try{
                    TcpMessenger messenger = new TcpMessenger(_requesterSocket);
                    messenger.sendTag(Tags.INDEXING_FAIL);
                } catch(IOException ex) { }
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

    static class SearchingMaster extends Thread {
        Socket _requesterSocket;

        HashSet<String> _unfinishedJobs;

        public SearchingMaster(Socket requesterSocket) {
            _requesterSocket = requesterSocket;
            _unfinishedJobs = new HashSet<String>();
        }

        void requestSearching(ServerInfo helper, ArrayList<String> keywords, String masterIpAddress, int masterPortNumber) throws IOException {
            Socket socket = new Socket(helper.IPAddressString(), helper.portNumber);
            TcpMessenger messenger = new TcpMessenger(socket);

            messenger.sendTag(Tags.REQUEST_SEARCHING);

            messenger.sendInt(keywords.size());
            for (String keyword : keywords) {
                messenger.sendString(keyword);
            }

            // Send the master IP and Port# to helper for it to report to.
            messenger.sendString(masterIpAddress);
            messenger.sendInt(masterPortNumber);
        }

        public void run() {
            try {


                // A messenger to talk to the one who requested this searching job.
                TcpMessenger messengerToRequester = new TcpMessenger(_requesterSocket);

                // Obtain the number of keywords to come.
                int numKeywords = messengerToRequester.receiveInt();

                // Obtain all the keywords.
                ArrayList<String> keywords = new ArrayList<String>();
                for (int i = 0; i < numKeywords; i++) {
                    keywords.add(messengerToRequester.receiveString());
                }

                // Group keywords by categories.
                HashMap<String, ArrayList<String>> groups = new HashMap<String, ArrayList<String>>();

                for (String keyword : keywords) {
                    String category = MiniGoogleUtilities.getCategoryOf(keyword);
                    if (groups.containsKey(category)) {
                        groups.get(category).add(keyword);
                    }
                    else {
                        ArrayList<String> keywordsUnderTheCategory = new ArrayList<String>();
                        keywordsUnderTheCategory.add(keyword);
                        groups.put(category, keywordsUnderTheCategory);
                    }
                }

                // Create a server socket for searching helpers to reply status.
                ServerSocket masterServerSocket = new ServerSocket(0);
                String masterIpAddress = Utilities.getMyIpAddress();
                int masterPortNumber = Utilities.getMyPortNumber(masterServerSocket);
                masterServerSocket.setSoTimeout(MAX_WAIT_TIME_FOR_HELPER);


                // For each category, borrow one searching helper from name server.
                for (Map.Entry<String, ArrayList<String>> pair : groups.entrySet()) {
                    String category = pair.getKey();
                    ServerInfo helper = MiniGoogleUtilities.borrowOneCategoriedHelper(category, _nameServerInfo);
                    requestSearching(helper, pair.getValue(), masterIpAddress, masterPortNumber);
                    _unfinishedJobs.add(category);
                }

                // Result container
                HashMap<String, ArrayList<Helper.PostingItem>> results = new HashMap<String, ArrayList<Helper.PostingItem>>();


                // Start to collect results.
                int numTrialsForSearching = 0;
                while (_unfinishedJobs.size() != 0 && numTrialsForSearching < MAX_TRIAL_NUM) {
                    Console.writeLine("@@@@@@@@@@@@@@@@@@ trying...");
                    try {
                        while (_unfinishedJobs.size() != 0) {
                            // Wait for helpers to respond.
                            Socket reducingHelperSocket = masterServerSocket.accept();
                            TcpMessenger reducingHelperMessenger = new TcpMessenger(reducingHelperSocket);

                            // Detect which category is finished
                            String finishedCategory = reducingHelperMessenger.receiveString();

                            Console.writeLine("\n" + finishedCategory + " is finished. Results are: ");

                            // Receive its results
                            int numWords = reducingHelperMessenger.receiveInt();
                            for (int i = 0; i < numWords; i++) {
                                String keyword = reducingHelperMessenger.receiveString();
                                String postingsString = reducingHelperMessenger.receiveString();
                                ArrayList<Helper.PostingItem> postings = MiniGoogleUtilities.stringToPostings(postingsString);
                                results.put(keyword, postings);
                            }
                            _unfinishedJobs.remove(finishedCategory);
                        }
                    }
                    catch (Exception e) {
                        numTrialsForSearching++;
                        Console.writeLine("\n");
                        for (String job : _unfinishedJobs) {
                            Console.writeLine(job);
                        }
                        Console.writeLine("\n");

                        for (String failedReducingJob : _unfinishedJobs) {
                            ServerInfo newReducingHelper = MiniGoogleUtilities.borrowOneCategoriedHelper(failedReducingJob, _nameServerInfo);
                            requestSearching(newReducingHelper, groups.get(failedReducingJob), masterIpAddress, masterPortNumber);
                        }
                    }
                }

                if (numTrialsForSearching == MAX_TRIAL_NUM) {
                    Console.writeLine("Give up");
                    throw new IOException("Searching failed. ");
                }

                // done receiving all results. Send to client
                for (Map.Entry<String, ArrayList<Helper.PostingItem>> entry : results.entrySet()) {
                    String keyword = entry.getKey();
                    ArrayList<Helper.PostingItem> postings = entry.getValue();
                    messengerToRequester.sendString(keyword);
                    messengerToRequester.sendString(MiniGoogleUtilities.postingsToString(postings));
                }

                TcpMessenger messenger = new TcpMessenger(_requesterSocket);
                messenger.sendTag(Tags.SEARCHING_DONE);


            }
            catch (IOException e) {
                Console.writeLine("IO error in searching master. ");
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
