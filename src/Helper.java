/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.collections.Pair;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Yuhuan Jiang on 11/27/14.
 */
public class Helper {

    // Configurations
    static final String MAPPER_OUT_DIR = "working/mappers/";

    static String _nameServerIpAddress;
    static int _nameServerPortNumber;

    static String _myIpAddress;
    static int _myPortNumber;

    static String _category;

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

    public static void main(String[] args) throws IOException {

        // Set up the listener on this helper.
        ServerSocket serverSocket = new ServerSocket(0);
        _myIpAddress = Utilities.getMyIpAddress();
        _myPortNumber = Utilities.getMyPortNumber(serverSocket);

        // TODO: Change the following to: read file to get name server's IP and Port #
        _nameServerIpAddress = "127.0.0.1";
        _nameServerPortNumber = 12345;
        Console.writeLine("A helper is running on " + _myIpAddress + " at port " + _myPortNumber + "\n");

        _category = register();

        // TODO: Load the partial II file for _category, if there is

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
               }
               else if (tag == Tags.REQUEST_INDEXING_REDUCING) {
               }
               else if (tag == Tags.REQUEST_SEARCHING) {
               }

           }
       }
       finally {
           serverSocket.close();
       }
    }

    private static HashMap<String, Integer> mapping(String[] lines) {
        HashMap<String, Integer> counts = new HashMap<String, Integer>();
        for (String line : lines) {
            String[] words = line.split("\\s+");
            for (String word : words) {
                int count = counts.containsKey(word) ? counts.get(word) : 0;
                counts.put(word, count + 1);
            }
        }
        return counts;
    }

    static class IndexingMappingWorker extends Thread {
        /**
         * The TPC socket of the client. Used to talk back to the client.
         */
        Socket _clientSocket;

        public IndexingMappingWorker(Socket clientSocket) {
            _clientSocket = clientSocket;
        }

        public void run() {
            try {
                Console.write("Start indexing mapping with ");

                TcpMessenger messenger = new TcpMessenger(_clientSocket);

                // Obtain the path to the file segment.
                String pathToSeg = messenger.receiveString();

                // Obtain the transaction ID.
                int transactionId = messenger.receiveInt();

                // Obtain part ID.
                int partId = messenger.receiveInt();

                Console.writeLine("transaction ID = " + transactionId + ", part ID = " + partId);


                // TODO: count words
                HashMap<String, Integer> counts = mapping(TextFile.read(pathToSeg));
                ArrayList<String> linesForOutput = new ArrayList<String>();
                for (HashMap.Entry<String, Integer> pair : counts.entrySet()) {
                    linesForOutput.add(pair.getKey() + "," + pair.getValue());
                }

                String[] outputLines = linesForOutput.toArray(new String[linesForOutput.size()]);

                // Save to file
                String pathToPartialCount = MAPPER_OUT_DIR + transactionId + "/" + partId;
                TextFile.write(pathToPartialCount, outputLines);

                // Inform the master (client) that the work is done
                //TODO: recover this: messenger.sendTag(Tags.STATUS_INDEXING_MAPPING_SUCCESS);

                Console.writeLine("Finished indexing mapping with transaction ID = " + transactionId + ", part ID = " + partId + "\n");
            }
            catch (IOException e) {
                Console.writeLine("IO error in indexing mapping worker. \n");
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

    private static class IndexingReducingWorker extends Thread {
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

                // Obtain the directory that mappers output the partial counts.
                String mapperOutputDir = messenger.receiveString();

                // Obtain the document name.
                String documentName = messenger.receiveString();

                // Open all partial count files in the directory.
                ArrayList<String> pathsToPartialCounts = Directory.getFiles(mapperOutputDir);
                ArrayList<String[]> partialCounts = new ArrayList<String[]>();
                for (String path : pathsToPartialCounts) {
                    partialCounts.add(TextFile.read(path));
                }

                // Turn all counts to a list of pairs: [Word, Count]
                ArrayList<Pair<String, Integer>> unmergedCounts = new ArrayList<Pair<String, Integer>>();
                for (String[] counts : partialCounts) {
                    for (String line : counts) {
                        String[] parts = line.split(",");
                        Pair<String, Integer> pair = new Pair<String, Integer>(parts[0], Integer.parseInt(parts[1]));
                        unmergedCounts.add(pair);
                    }
                }

                // Merge partial counts
                HashMap<String, Integer> mergedCounts = new HashMap<String, Integer>();

                for (Pair<String, Integer> pair : unmergedCounts) {
                    String curWord = pair.item1;
                    int count = mergedCounts.containsKey(curWord) ? mergedCounts.get(curWord) : 0;
                    mergedCounts.put(curWord, count + pair.item2);
                }

                HashMap<String, Pair<String, Integer>> currentInvertedIndex = new HashMap<String, Pair<String, Integer>>();
                for (HashMap.Entry<String, Integer> pair : mergedCounts.entrySet()) {
                    currentInvertedIndex.put(pair.getKey(), new Pair<String, Integer>(documentName, pair.getValue()));
                }

                // Merge with the II already in the category.

                Console.writeLine("Finished indexing mapping. \n");

            }
            catch (IOException e) {
                Console.writeLine("IO error in indexing reducing worker. \n");
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
