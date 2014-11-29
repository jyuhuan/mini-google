/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.collections.MultiValueHashTable;
import me.yuhuan.collections.Pair;
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
 * Created by Yuhuan Jiang on 11/24/14.
 */
public class NameServer {

    // Configurations
    private static final int PORT_NUMBER = 12345; // TODO: change to 0

    public static class MiniGoogleNameServerTable {
        volatile ConcurrentHashMap<String, ArrayList<Pair<ServerInfo, Integer>>> _table;

        public MiniGoogleNameServerTable(ArrayList<String> keys) {
            _table = new ConcurrentHashMap<String, ArrayList<Pair<ServerInfo, Integer>>>();
            for (String key : keys) {
                _table.put(key, new ArrayList<Pair<ServerInfo, Integer>>());
            }
        }

        /**
         * Gets the next category according to some standard. Currently, it returns the category
         * that has the least number of helpers.
         * @return The category that has the least number of helpers.
         */
        private String getNextCategory() {
            int minNumHelpers = Integer.MAX_VALUE;
            String categoryThatHasMinNumHelpers = "";
            for (Map.Entry<String, ArrayList<Pair<ServerInfo, Integer>>> entry : _table.entrySet()) {
                int curNumHelpers = entry.getValue().size();
                if (curNumHelpers < minNumHelpers) {
                    minNumHelpers = curNumHelpers;
                    categoryThatHasMinNumHelpers = entry.getKey();
                }
            }
            return categoryThatHasMinNumHelpers;
        }

        public synchronized String addHelper(ServerInfo serverInfo) {
            String categoryToAddFor = getNextCategory();

            // A server to be added should be a server that is just launched, with 0 threads running.
            _table.get(categoryToAddFor).add(new Pair<ServerInfo, Integer>(serverInfo, 0));
            return categoryToAddFor;
        }

        //region Server Load Managing

        private synchronized void increaseLoadOn(ServerInfo serverInfo) {
            for (Map.Entry<String, ArrayList<Pair<ServerInfo, Integer>>> entry : _table.entrySet()) {
                for (Pair<ServerInfo, Integer> pair : entry.getValue()) {
                    if (pair.item1.equals(serverInfo)) {
                        pair.item2++;
                    }
                }
            }
        }

        private synchronized void increaseLoadOn(ServerInfo serverInfo, String category) {
            for (Pair<ServerInfo, Integer> pair : _table.get(category)) {
                if (pair.item1.equals(serverInfo)) {
                    pair.item2++;
                }
            }
        }

        private synchronized void decreaseLoadOn(ServerInfo serverInfo) {
            for (Map.Entry<String, ArrayList<Pair<ServerInfo, Integer>>> entry : _table.entrySet()) {
                for (Pair<ServerInfo, Integer> pair : entry.getValue()) {
                    if (pair.item1.equals(serverInfo)) {
                        pair.item2--;
                    }
                }
            }
        }

        private synchronized void decreaseLoadOn(ServerInfo serverInfo, String category) {
            for (Pair<ServerInfo, Integer> pair : _table.get(category)) {
                if (pair.item1.equals(serverInfo)) {
                    pair.item2--;
                }
            }
        }

        //endregion

        /**
         * Gets a server that is the least busy.
         */
        public synchronized ServerInfo  borrowHelper() {
            int lowestLoad = Integer.MAX_VALUE;
            ServerInfo mostIdle = null;
            for (Map.Entry<String, ArrayList<Pair<ServerInfo, Integer>>> entry : _table.entrySet()) {
                for (Pair<ServerInfo, Integer> pair : entry.getValue()) {
                    ServerInfo serverInfo = pair.item1;
                    int load = pair.item2;
                    if (load < lowestLoad) {
                        lowestLoad = load;
                        mostIdle = serverInfo;
                    }
                }
            }
            increaseLoadOn(mostIdle);
            return mostIdle;
        }

        public synchronized void returnHelper(ServerInfo serverInfo) {
            decreaseLoadOn(serverInfo);
        }

        /**
         * Gets a server in a category that is the least busy.
         * @param category A category to search a server for.
         * @return A server in the specified category that is the least busy.
         */
        public synchronized ServerInfo borrowHelper(String category) {
            int lowestLoad = Integer.MAX_VALUE;
            ServerInfo mostIdle = null;
            for (Pair<ServerInfo, Integer> pair : _table.get(category)) {
                ServerInfo serverInfo = pair.item1;
                int load = pair.item2;
                if (load < lowestLoad) {
                    lowestLoad = load;
                    mostIdle = serverInfo;
                }
            }
            increaseLoadOn(mostIdle, category);
            return mostIdle;
        }

        public synchronized void returnHelper(ServerInfo serverInfo, String category) {
            decreaseLoadOn(serverInfo, category);
        }
    }


    static MiniGoogleNameServerTable _table;

    private static ArrayList<String> generateCategories() {
        HashMap<String, Integer> sizes = new HashMap<String, Integer>();

        sizes.put("s", 56);
        sizes.put("c", 50);
        sizes.put("p", 40);
        sizes.put("b", 32);
        sizes.put("d", 31);
        sizes.put("a", 31);
        sizes.put("m", 28);
        sizes.put("t", 27);
        sizes.put("r", 26);
        sizes.put("f", 25);
        sizes.put("e", 22);
        sizes.put("i", 21);
        sizes.put("h", 21);
        sizes.put("l", 19);
        sizes.put("g", 18);
        sizes.put("w", 17);
        sizes.put("u", 15);
        sizes.put("o", 13);
        sizes.put("v", 12);
        sizes.put("n", 11);
        sizes.put("j", 5);
        sizes.put("#", 4);
        sizes.put("k", 4);
        sizes.put("q", 3);
        sizes.put("y", 2);
        sizes.put("x", 2);
        sizes.put("z", 1);

        ArrayList<String> categories = new ArrayList<String>();
        for (HashMap.Entry<String, Integer> entry : sizes.entrySet()) {
            String item = entry.getKey();
            int count = entry.getValue();
            for (int i = 0; i < count; i++) {
                categories.add(item + i);
            }
        }

        return categories;
    }

    private static ArrayList<String> generateSimpleCategories() {
        ArrayList<String> categories = new ArrayList<String>();
        categories.add("cat1");
        categories.add("cat2");
        categories.add("cat3");
        categories.add("cat4");
        categories.add("cat5");
        return categories;
    }

    public static void main(String[] args) throws IOException {

        // Generate a categories
        ArrayList<String> categories = generateSimpleCategories();

        // Create the table with the categories as keys, and value being empty.
        _table = new MiniGoogleNameServerTable(categories);

        // A socket that listens to incoming requests on a system-allocated port number.
        ServerSocket serverSocket = new ServerSocket(PORT_NUMBER);

        // Figure out the IP address and port number of this name server.
        String myIpAddress = Utilities.getMyIpAddress();
        int myPortNumber = Utilities.getMyPortNumber(serverSocket);

        Console.writeLine("A name server is now running on " + myIpAddress + ", at port " + myPortNumber + "\n");

        // Write the IP and port number of this port mapper to a publicly known file location.
        TextFile.write("name_server_info", new String[]{myIpAddress, String.valueOf(myPortNumber)});

        // TODO: start the "I'm Alive" worker here.

        try {
            while (true) {
                // Get the client's TCP socket.
                Socket clientSocket = serverSocket.accept();
                TcpMessenger messenger = new TcpMessenger(clientSocket);


                // Read the tag received. The tag should be one of the following:
                //     Tags.REQUEST_HELPER_REGISTERING
                //     Tags.REQUEST_INDEXING
                //     Tags.REQUEST_SEARCHING

                int tag = messenger.receiveTag();
                if (tag == Tags.REQUEST_HELPER_REGISTERING) {
                    // Print who wants to register
                    Console.writeLine("Helper " + clientSocket + " wants to register. ");

                    // Start registration worker
                    (new RegistrationWorker(clientSocket)).start();
                }
                else if (tag == Tags.REQUEST_INDEXING) {
                    // Print who wants to do indexing
                    Console.writeLine("Client " + clientSocket + " wants to index a document. ");
                    // TODO: start indexing master
                }
                else if (tag == Tags.REQUEST_SEARCHING) {
                    // Print who wants to do searching
                    Console.writeLine("Client " + clientSocket + " wants to search for some keywords. ");
                    // TODO: start searching master
                }

            }
        }
        finally {
            serverSocket.close();
        }
    }

    /**
     * A worker that registers a helper in the table of name server.
     */
    private static class RegistrationWorker extends Thread {
        Socket _clientSocket;

        public RegistrationWorker(Socket clientSocket) {
            _clientSocket = clientSocket;
        }

        public void run() {
            try {
                TcpMessenger messenger = new TcpMessenger(_clientSocket);

                // Obtain the helper to be registered, and add it.
                ServerInfo serverInfo = messenger.receiveServerInfo();
                String categoryAssigned = _table.addHelper(serverInfo);

                // Inform the helper which category it is responsible for.
                messenger.sendString(categoryAssigned);

                // Print the registration on terminal.
                Console.writeLine(_clientSocket.getInetAddress().getHostAddress() + " at " + _clientSocket.getPort() + " is registered and assigned category " + categoryAssigned);
            }
            catch (IOException e) {
                Console.writeLine("IO error in registration worker. ");
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

    private static class MappingHelperLookupWorker extends Thread {
        Socket _clientSocket;

        public MappingHelperLookupWorker(Socket clientSocket) {
            _clientSocket = clientSocket;
        }

        public void run() {
            try {
                TcpMessenger messenger = new TcpMessenger(_clientSocket);

                // Obtain how many mapping helpers the requester wants.
                int numHelpersReqested = messenger.receiveInt();

                // Borrow that many helpers from the table.
                ArrayList<ServerInfo> helpers = new ArrayList<ServerInfo>();
                for (int i = 0; i < numHelpersReqested; i++) {
                    helpers.add(_table.borrowHelper());
                }

                // Send these helpers to the requester.
                messenger.sendServerInfoArray(helpers);
            }
            catch (IOException e) {
                Console.writeLine("IO error in mapping helper lookup worker. ");
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
