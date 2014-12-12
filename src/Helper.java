/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.collections.Pair;
import me.yuhuan.io.Directory;
import me.yuhuan.io.File;
import me.yuhuan.io.TextFile;
import me.yuhuan.net.Utilities;
import me.yuhuan.net.core.ServerInfo;
import me.yuhuan.net.core.TcpMessenger;
import me.yuhuan.utilities.Console;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Yuhuan Jiang on 11/27/14.
 */

/**
 * A helper in the MapReduce framework. A helper can play three roles, depending on the request.
 * The three roles are: <ol>
 *     <li> Mapper helper for indexing </li>
 *     <li> Reducing helper for indexing </li>
 *     <li> Searching helper </li>
 * </ol>
 */
public class Helper {

    public static class PostingItem implements Comparable<PostingItem> {
        Pair<String, Integer> _item;

        public PostingItem(String documentName, Integer frequency) {
            _item = new Pair<String, Integer>(documentName, frequency);
        }

        public String getDocumentName() { return _item.item1; }

        public int getFrequency() { return _item.item2; }

        @Override
        public int compareTo(PostingItem that) {
            int thisFreq = this._item.item2;
            int thatFreq = that._item.item2;
            return thisFreq - thatFreq;
        }

        @Override
        public String toString() {
            return _item.toString();
        }
    }



    /**
     * Represents an inverted index (II) in MapReduce.
     * Allows loading from and saving to files.
     * Allows merging with counts for new documents.
     */
    public static class InvertedIndex {

        /**
         * [("word", [("doc1", 123), ("doc2", 90), ... ]), ...]
         */
        volatile ConcurrentHashMap<String, ArrayList<PostingItem>> _table;

        /**
         * Initializes an empty inverted index.
         */
        public InvertedIndex() {
            _table = new ConcurrentHashMap<String, ArrayList<PostingItem>>();
        }

        /**
         * Initializes the inverted index by loading a file.
         * @param filePath The path to the II's file.
         */
        public InvertedIndex(String filePath) throws IOException {
            _table = new ConcurrentHashMap<String, ArrayList<PostingItem>>();
            String[] lines = TextFile.read(filePath);
            for (String line : lines) {
                String[] parts = line.split(",");
                String word = parts[0];
                ArrayList<PostingItem> postings = new ArrayList<PostingItem>();
                for (int i = 1; i < parts.length; i++) {
                    String[] docIdAndCount = parts[i].split("\\|");
                    String docId = docIdAndCount[0];
                    int count = Integer.parseInt(docIdAndCount[1]);
                    postings.add(new PostingItem(docId, count));
                }
                _table.put(word, postings);
            }
        }

        public ArrayList<PostingItem> get(String word) {
            return _table.get(word);
        }

        /**
         * Merge with the counts for a new document.
         * @param more Counts for the new document.
         * @param documentName Name of the new document.
         */
        public void mergeWith(HashMap<String, Integer> more, String documentName) {
            try {
                for (Map.Entry<String, Integer> pair : more.entrySet()) {
                    String word = pair.getKey();
                    int count = pair.getValue();

                    if (_table.containsKey(word)) {
                        _table.get(word).add(new PostingItem(documentName, count));
                    } else {
                        ArrayList<PostingItem> postings = new ArrayList<PostingItem>();
                        postings.add(new PostingItem(documentName, count));
                        _table.put(word, postings);
                    }
                }
            }
            catch (NullPointerException e) {
                int aaa = 0;
            }
        }

        /**
         * Saves the inverted index to a text file.
         * @param filePath Where to save.
         */
        public synchronized void saveToFile(String filePath) throws IOException{
            ArrayList<String> lines = new ArrayList<String>();
            for (Map.Entry<String, ArrayList<PostingItem>> pair : _table.entrySet()) {
                String word = pair.getKey();
                ArrayList<PostingItem> postings = pair.getValue();

                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(word);
                stringBuilder.append(",");
                for (PostingItem posting : postings) {
                    stringBuilder.append(posting.getDocumentName());
                    stringBuilder.append("|");
                    stringBuilder.append(posting.getFrequency());
                    stringBuilder.append(",");
                }
                String postingsString = stringBuilder.substring(0, stringBuilder.length() - 1);
                lines.add(postingsString);
            }
            TextFile.write(filePath, lines);
        }
    }

    //region HELPER CONFIGURATIONS
    static final String MAPPER_OUT_DIR = "working/mappers/";
    static final String REDUCER_DIR = "working/reducers/";
    static final int IM_ALIVE_INTERVAL = 5000;
    //endregion

    static String _nameServerIpAddress;
    static int _nameServerPortNumber;

    static String _myIpAddress;
    static int _myPortNumber;

    static String _category;

    static InvertedIndex _invertedIndex;

    /**
     * Registers this helper to the name server, and obtains the category assigned by the name server.
     * @return The category assigned by the name server.
     * @throws IOException
     */
    static String register() throws IOException {
        Console.writeLine("Registering myself... ");
        Socket socketToNameServer = new Socket(_nameServerIpAddress, _nameServerPortNumber);
        TcpMessenger messenger = new TcpMessenger(socketToNameServer);
        messenger.sendTag(Tags.REQUEST_HELPER_REGISTERING);
        messenger.sendServerInfo(new ServerInfo(_myIpAddress, _myPortNumber));
        String categoryAssigned = messenger.receiveString();
        Console.writeLine("Registration success. I am responsible for category " + categoryAssigned + "\n");
        return categoryAssigned;
    }

    static void returnToNameServer() throws IOException {
        try {
            Socket socketToNameServer = new Socket(_nameServerIpAddress, _nameServerPortNumber);
            TcpMessenger messenger = new TcpMessenger(socketToNameServer);
            messenger.sendTag(Tags.REQUEST_HELPER_RETURN);
            messenger.sendServerInfo(new ServerInfo(_myIpAddress, _myPortNumber));
            messenger.sendString(_category);
        }
        catch (Exception e) {
            int aaa = 0;
        }
    }

    public static void main(String[] args) throws IOException {

        // Set up the listener on this helper.
        ServerSocket serverSocket = new ServerSocket(0);
        _myIpAddress = Utilities.getMyIpAddress();
        _myPortNumber = Utilities.getMyPortNumber(serverSocket);

        String[] lines = TextFile.read("name_server_info");
        _nameServerIpAddress = lines[0];
        _nameServerPortNumber = Integer.parseInt(lines[1]);

        Console.writeLine("A helper is running on " + _myIpAddress + " at port " + _myPortNumber + "\n");

        _category = register();

        // Load the partial II file for _category, if there is
        String pathToPartialInvertedIndex = REDUCER_DIR + _category;
        if (TextFile.exists(pathToPartialInvertedIndex)) {
            _invertedIndex = new InvertedIndex(pathToPartialInvertedIndex);
        }
        else {
            _invertedIndex = new InvertedIndex();
        }

        // Start the reporter thread that tells the name server I'm alive.
        (new ImAliveWorker()).start();

        try {
            while (true) {
                // Obtain the client's TCP socket.
                Socket clientSocket = serverSocket.accept();

                // Create IO wrapper for the client's socket.
                TcpMessenger messenger = new TcpMessenger(clientSocket);

                // Determine the type of request. Possible types are:
                //    (1) Indexing mapping
                //    (2) Indexing reducing
                //    (3) Searching
                int tag = messenger.receiveTag();
                if (tag == Tags.REQUEST_INDEXING_MAPPING) {
                    (new IndexingMappingWorker(clientSocket)).start();
                } else if (tag == Tags.REQUEST_INDEXING_REDUCING) {
                    (new IndexingReducingWorker(clientSocket)).start();
                } else if (tag == Tags.REQUEST_SEARCHING) {
                    (new SearchingWorker(clientSocket)).start();
                }

            }
        } finally {
            serverSocket.close();
        }
    }

    private static HashMap<String, Integer> mapping(String[] lines) {
        HashMap<String, Integer> counts = new HashMap<String, Integer>();
        for (String line : lines) {
            String[] rawWords = line.split("\\s+");

            // valid words. without @#$ and other blank words
            ArrayList<String> words = new ArrayList<String>();
            for (String word : rawWords) {
                if (MiniGoogleUtilities.isWord(word)) words.add(word);
            }

            for (String word : words) {
                int count = counts.containsKey(word) ? counts.get(word) : 0;
                counts.put(word, count + 1);
            }
        }
        return counts;
    }

    static class ImAliveWorker extends Thread {
        public void run() {

            try {
                while (true) {

                    Socket socketToNameServer = new Socket(_nameServerIpAddress, _nameServerPortNumber);
                    TcpMessenger messenger = new TcpMessenger(socketToNameServer);

                    messenger.sendTag(Tags.MESSAGE_HELPER_ALIVE);
                    messenger.sendServerInfo(new ServerInfo(_myIpAddress, _myPortNumber));

                    try {
                        Thread.sleep(IM_ALIVE_INTERVAL);
                    }
                    catch (InterruptedException e) { }
                }

            } catch (IOException e) {
                Console.writeLine("IO error in the I'm Alive! worker. \n");
            }
        }
    }

    static class IndexingMappingWorker extends Thread {
        /**
         * The TPC socket of the client. Used to talk back to the client.
         */
        Socket _clientSocket;

        public IndexingMappingWorker(Socket clientSocket) {
            _clientSocket = clientSocket;
        }

        String _workingPath;
        int _transactionId;
        String _masterIp;
        int _masterPort;
        boolean _didWriteFile = false;
        boolean _didReportToMaster = false;
        boolean _didReturnToNs = false;

        public void run() {

            try {
                Console.write("Start indexing mapping with ");

                TcpMessenger messenger = new TcpMessenger(_clientSocket);

                // Obtain the path to the file segment.
                String pathToSeg = messenger.receiveString();
                _workingPath = pathToSeg;

                // Obtain the transaction ID.
                int transactionId = messenger.receiveInt();
                Console.writeLine("transaction ID = " + transactionId);
                _transactionId = transactionId;

                // Obtain the IP and Port# of the master.
                String masterIpAddress = messenger.receiveString();
                int masterPortNumber = messenger.receiveInt();

                _masterIp = masterIpAddress;
                _masterPort = masterPortNumber;

                // Create a socket and a messenger for this helper to report finishing to.
                Socket socketToMaster = new Socket(masterIpAddress, masterPortNumber);
                TcpMessenger messengerToMaster = new TcpMessenger(socketToMaster);

                HashMap<String, Integer> counts = mapping(TextFile.read(pathToSeg));
                ArrayList<String> linesForOutput = new ArrayList<String>();
                for (Map.Entry<String, Integer> pair : counts.entrySet()) {
                    linesForOutput.add(pair.getKey() + "," + pair.getValue());
                }

                String[] outputLines = linesForOutput.toArray(new String[linesForOutput.size()]);

                // Save to file
                String pathToPartialCount = MAPPER_OUT_DIR + transactionId + "/" + File.extractFileNameFromPath(pathToSeg);
                TextFile.write(pathToPartialCount, outputLines);
                _didWriteFile = true;

                // Inform the master (client) that the work is done
                messengerToMaster.sendString(pathToSeg);
                _didReportToMaster = true;

                // Return myself to name server
                returnToNameServer();
                _didReturnToNs = true;

                Console.writeLine("Finished indexing mapping with transaction ID = " + transactionId + "\n\t" +
                                " Working path = " + _workingPath + "\n\t" +
                                "Trans ID = " + _transactionId + "\n"
                );

            } catch (IOException e) {
                Console.writeLine("IO error in indexing mapping worker. \n\t" +
                        e.getMessage() + "\n\t" +
                        " Working path = " + _workingPath + "\n\t" +
                                "Trans ID = " + _transactionId + "\n\t" +
                                "Master = " + _masterIp + ":" + _masterPort + "\n\t" +
                                "Did write file = " + _didWriteFile + "\n\t" +
                                "Did report to master = " + _didReportToMaster + "\n\t" +
                                "Did return to NS = " + _didReturnToNs + "\n"
                );
            } finally {
                try {
                    _clientSocket.close();
                    Console.writeLine("Socket to client " + _clientSocket.getInetAddress().getHostAddress() + ":" + _clientSocket.getPort() + " is closed. ");
                } catch (IOException e) {
                    Console.writeLine("Socket to client " + _clientSocket.getInetAddress().getHostAddress() + ":" + _clientSocket.getPort() + " failed to close. ");
                }
            }
        }
    }

    static class IndexingReducingWorker extends Thread {
        /**
         * The TPC socket of the client. Used to talk back to the client.
         */
        Socket _clientSocket;

        public IndexingReducingWorker(Socket clientSocket) {
            _clientSocket = clientSocket;
        }

        public void run() {
            try {
                Console.write("Start indexing reducing. ");
                TcpMessenger messenger = new TcpMessenger(_clientSocket);

                // Obtain the transactionId that points to the directory where mappers have output the partial counts.
                int transactionId = messenger.receiveInt();

                // Obtain the document name.
                String documentName = messenger.receiveString();

                // Obtain the IP and Port# of the master.
                String masterIpAddress = messenger.receiveString();
                int masterPortNumber = messenger.receiveInt();

                // Create a socket and a messenger for this helper to report finishing to.
                Socket socketToMaster = new Socket(masterIpAddress, masterPortNumber);
                TcpMessenger messengerToMaster = new TcpMessenger(socketToMaster);

                // Open all partial count files in the directory.
                ArrayList<String> pathsToPartialCounts = Directory.getFiles(MAPPER_OUT_DIR + transactionId + "/");
                ArrayList<String[]> partialCounts = new ArrayList<String[]>();
                for (String path : pathsToPartialCounts) {
                    partialCounts.add(TextFile.read(path));
                }

                // Turn all counts to a list of pairs: [Word, Count]
                ArrayList<Pair<String, Integer>> unmergedCounts = new ArrayList<Pair<String, Integer>>();
                for (String[] counts : partialCounts) {
                    for (String line : counts) {
                        String[] parts = line.split(",");
                        String word = parts[0];

                        if (MiniGoogleUtilities.getCategoryOf(word).equals(_category)) {
                            Pair<String, Integer> pair = new Pair<String, Integer>(word, Integer.parseInt(parts[1]));
                            unmergedCounts.add(pair);
                        }
                    }
                }

                // Merge partial counts
                HashMap<String, Integer> combinedCounts = new HashMap<String, Integer>();

                for (Pair<String, Integer> pair : unmergedCounts) {
                    String curWord = pair.item1;
                    int count = combinedCounts.containsKey(curWord) ? combinedCounts.get(curWord) : 0;
                    combinedCounts.put(curWord, count + pair.item2);
                }

                // Merge combinedCounts with _invertedIndex, the II already calculated for this category.
                _invertedIndex.mergeWith(combinedCounts, documentName);

                // Write _invertedIndex to file.
                _invertedIndex.saveToFile(REDUCER_DIR + _category);

                // Inform the master
                messengerToMaster.sendString(_category);

                // Return myself to name server
                returnToNameServer();

                Console.writeLine("Finished indexing reducing. \n");

            } catch (IOException e) {
                Console.writeLine("IO error in indexing reducing worker. \n");
            } finally {
                try {
                    _clientSocket.close();
                    Console.writeLine("Socket to client " + _clientSocket.getInetAddress().getHostAddress() + ":" + _clientSocket.getPort() + " is closed. ");
                } catch (IOException e) {
                    Console.writeLine("Socket to client " + _clientSocket.getInetAddress().getHostAddress() + ":" + _clientSocket.getPort() + " failed to close. ");
                }
            }
        }

    }

    static class SearchingWorker extends Thread {
        Socket _clientSocket;

        public SearchingWorker(Socket clientSocket) {
            _clientSocket = clientSocket;
        }

        public void run() {
            try {

                Console.writeLine("Searching starts. ");

                TcpMessenger messenger = new TcpMessenger(_clientSocket);

                // Obtain the number of keywords to come.
                int numKeywords = messenger.receiveInt();

                // Obtain all the keywords.
                ArrayList<String> keywords = new ArrayList<String>();
                for (int i = 0; i < numKeywords; i++) {
                    keywords.add(messenger.receiveString());
                }

                // Obtain the IP and Port# of the master.
                String masterIpAddress = messenger.receiveString();
                int masterPortNumber = messenger.receiveInt();

                // Create a socket and a messenger for this helper to report finishing to.
                Socket socketToMaster = new Socket(masterIpAddress, masterPortNumber);
                TcpMessenger messengerToMaster = new TcpMessenger(socketToMaster);
                messengerToMaster.sendString(_category);


                // Look up table, and output result.
                for (String keyword : keywords) {
                    Console.write("Results for keyword " + keyword + " are: ");
                    Console.writeLine("\t" + _invertedIndex.get(keyword));
                }

                // Return the postings to master
                messengerToMaster.sendInt(keywords.size());
                for (String keyword : keywords) {
                    messengerToMaster.sendString(keyword);
                    String postingsString = MiniGoogleUtilities.postingsToString(_invertedIndex.get(keyword));
                    messengerToMaster.sendString(postingsString);
                }

                // Return myself to name server
                returnToNameServer();


                Console.writeLine("Searching done. ");

            } catch (IOException e) {
                Console.writeLine("IO error in indexing reducing worker. \n");
            } finally {
                try {
                    _clientSocket.close();
                    Console.writeLine("Socket to client " + _clientSocket.getInetAddress().getHostAddress() + ":" + _clientSocket.getPort() + " is closed. ");
                } catch (IOException e) {
                    Console.writeLine("Socket to client " + _clientSocket.getInetAddress().getHostAddress() + ":" + _clientSocket.getPort() + " failed to close. ");
                }
            }
        }
    }

}
