/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

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
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Yuhuan Jiang on 11/24/14.
 */
public class NameServer {

    // Configurations
    private static final int PORT_NUMBER = 0;
    private static final int CHECK_INTERVAL = 3000;
    private static final long HELPER_ALIVE_THREASHOLD = 8000;

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
         *
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
        public synchronized ServerInfo borrowHelper() {
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
         *
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

        public synchronized void removeHelper(ServerInfo serverInfo) {
            for (Map.Entry<String, ArrayList<Pair<ServerInfo, Integer>>> entry : _table.entrySet()) {
                ArrayList<Pair<ServerInfo, Integer>> list = entry.getValue();
                Pair<ServerInfo, Integer> toRemove = null;
                for (Pair<ServerInfo, Integer> pair : list) {
                    if (pair.item1.equals(serverInfo)) toRemove = pair;
                }
                if (toRemove != null) list.remove(toRemove);
            }
        }

        @Override
        public synchronized String toString() {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<String, ArrayList<Pair<ServerInfo, Integer>>> entry : _table.entrySet()) {
                builder.append(entry.getKey() + ":\t");
                for (Pair<ServerInfo, Integer> pair : entry.getValue()) {
                    builder.append(pair.toString() + "\t|| ");
                }
                builder.append("\n");
            }
            return builder.toString();
        }
    }

    public static class MiniGoogleServerTracker {
        volatile ConcurrentHashMap<ServerInfo, Long> _table;

        public MiniGoogleServerTracker() {
            _table = new ConcurrentHashMap<ServerInfo, Long>();
        }

        private long getCurrentTime() {
            Date date = new Date();
            return date.getTime();
        }

        public synchronized void add(ServerInfo serverInfo) {
            if (!_table.containsKey(serverInfo)) _table.put(serverInfo, getCurrentTime());
        }

        public synchronized void remove(ServerInfo serverInfo) {
            if (_table.containsKey(serverInfo)) _table.remove(serverInfo);
        }

        public synchronized void retain(ServerInfo serverInfo) {
            if (_table.containsKey(serverInfo)) _table.put(serverInfo, getCurrentTime());
        }

        public synchronized boolean isDead(ServerInfo serverInfo) {
            if (_table.containsKey(serverInfo)) {
                long lastTimestamp = _table.get(serverInfo);
                long currentTimestamp = getCurrentTime();
                return (currentTimestamp - lastTimestamp > HELPER_ALIVE_THREASHOLD);
            }
            else return false;
        }

        public synchronized ArrayList<ServerInfo> getDeadServers() {
            ArrayList<ServerInfo> result = new ArrayList<ServerInfo>();
            for (Map.Entry<ServerInfo, Long> pair : _table.entrySet()) {
                ServerInfo curServer = pair.getKey();
                if (isDead(curServer)) result.add(curServer);
            }
            return result;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<ServerInfo, Long> pair : _table.entrySet()) {
                builder.append(pair.getKey());
                builder.append(" : ");
                builder.append(pair.getValue());
                builder.append("\n");
            }
            return builder.toString();
        }
    }

    static MiniGoogleNameServerTable _table;
    static MiniGoogleServerTracker _serverTracker;

    public static void main(String[] args) throws IOException {

        // Generate a categories
        ArrayList<String> categories = MiniGoogleUtilities.generateCategories();

        // Create the table with the categories as keys, and value being empty.
        _table = new MiniGoogleNameServerTable(categories);

        // Create a server tracker that tracks the liveliness of each helper.
        _serverTracker = new MiniGoogleServerTracker();

        // A socket that listens to incoming requests on a system-allocated port number.
        ServerSocket serverSocket = new ServerSocket(PORT_NUMBER);

        // Figure out the IP address and port number of this name server.
        String myIpAddress = Utilities.getMyIpAddress();
        int myPortNumber = Utilities.getMyPortNumber(serverSocket);

        Console.writeLine("A name server is now running on " + myIpAddress + ", at port " + myPortNumber + "\n");

        // Write the IP and port number of this port mapper to a publicly known file location.
        TextFile.write("name_server_info", new String[]{myIpAddress, String.valueOf(myPortNumber)});

        (new HelperAvailabilityCheckingWorker()).start();

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
                } else if (tag == Tags.REQUEST_CATEGORY_HELPER) {
                    Console.writeLine("Client " + clientSocket + " requests a helper from a certain category. ");
                    (new CategoriedHelperLookupWorker(clientSocket)).start();
                } else if (tag == Tags.REQUEST_A_SET_OF_CATEGORY_HELPER) {
                    Console.writeLine("Client " + clientSocket + " requests a set of categoried helpers. ");
                    (new CategoriedHelperSetLookupWorker(clientSocket)).start();
                } else if (tag == Tags.REQUEST_CATEGORYLESS_HELPER) {
                    Console.writeLine("Client " + clientSocket + " requests a helper from any category. ");
                    (new CategorylessHelperLookupWorker(clientSocket)).start();
                } else if (tag == Tags.REQUEST_HELPER_RETURN) {
                    Console.writeLine("Client " + clientSocket + " wants to return a helper. ");
                    (new HelperReturnWorker(clientSocket)).start();
                }
                else if (tag == Tags.MESSAGE_HELPER_ALIVE) {
                    (new HelperAvailabilityUpdatingWorker(clientSocket)).start();
                }
            }
        } finally {
            serverSocket.close();
        }
    }

    static class HelperAvailabilityUpdatingWorker extends Thread {
        Socket _helperSocket;

        public HelperAvailabilityUpdatingWorker(Socket helperSocket) {
            _helperSocket = helperSocket;
        }

        public void run() {
            try {
                TcpMessenger messenger = new TcpMessenger(_helperSocket);
                ServerInfo serverInfoToUpdate = messenger.receiveServerInfo();

                _serverTracker.retain(serverInfoToUpdate);

            } catch (IOException e) {
                Console.writeLine("IO error in helper updating worker. ");
            } finally {
                try {
                    _helperSocket.close();
                    //Console.writeLine("Socket to client " + _helperSocket.getInetAddress().getHostAddress() + ":" + _helperSocket.getPort() + " is closed. ");
                } catch (IOException e) {
                    //Console.writeLine("Socket to client " + _helperSocket.getInetAddress().getHostAddress() + ":" + _helperSocket.getPort() + " failed to close. ");
                }
            }
        }
    }

    static class HelperAvailabilityCheckingWorker extends Thread {

        public void run() {

            while (true) {

                ArrayList<ServerInfo> deadHelpers = _serverTracker.getDeadServers();
                for (ServerInfo serverInfo : deadHelpers) {
                    _table.removeHelper(serverInfo);
                    _serverTracker.remove(serverInfo);
                }

                Console.writeLine("\n Table: ");
                Console.writeLine(_table.toString());

                //Console.writeLine("\n Tracker: ");
                //Console.writeLine(_serverTracker.toString());


                try {
                    Thread.sleep(CHECK_INTERVAL);
                } catch (InterruptedException e) {
                }

            }

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

                // Obtain the helper to be registered, and add it to the table and server tracker.
                ServerInfo serverInfo = messenger.receiveServerInfo();
                String categoryAssigned = _table.addHelper(serverInfo);
                _serverTracker.add(serverInfo);

                // Inform the helper which category it is responsible for.
                messenger.sendString(categoryAssigned);

                // Print the registration on terminal.
                Console.writeLine(_clientSocket.getInetAddress().getHostAddress() + " at " + _clientSocket.getPort() + " is registered and assigned category " + categoryAssigned);
            } catch (IOException e) {
                Console.writeLine("IO error in registration worker. ");
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

    private static class CategorylessHelperLookupWorker extends Thread {
        Socket _clientSocket;

        public CategorylessHelperLookupWorker(Socket clientSocket) {
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
            } catch (IOException e) {
                Console.writeLine("IO error in mapping helper lookup worker. ");
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

    private static class CategoriedHelperLookupWorker extends Thread {
        Socket _clientSocket;

        public CategoriedHelperLookupWorker(Socket clientSocket) {
            _clientSocket = clientSocket;
        }

        public void run() {
            try {
                TcpMessenger messenger = new TcpMessenger(_clientSocket);

                // Obtain what category the requester wants.
                String category = messenger.receiveString();

                // Borrow that many helpers from the table.
                ServerInfo helper = _table.borrowHelper(category);

                // Send these helpers to the requester.
                messenger.sendServerInfo(helper);
            } catch (IOException e) {
                Console.writeLine("IO error in one categoried helper lookup worker. ");
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


    private static class CategoriedHelperSetLookupWorker extends Thread {
        Socket _clientSocket;

        public CategoriedHelperSetLookupWorker(Socket clientSocket) {
            _clientSocket = clientSocket;
        }

        public void run() {
            try {
                TcpMessenger messenger = new TcpMessenger(_clientSocket);

                // Borrow that many helpers from the table.
                ArrayList<ServerInfo> helpers = new ArrayList<ServerInfo>();
                for (String category : MiniGoogleUtilities.generateCategories()) {
                    helpers.add(_table.borrowHelper(category));
                }

                // Send these helpers to the requester.
                messenger.sendServerInfoArray(helpers);
            } catch (IOException e) {
                Console.writeLine("IO error in categoried helper set lookup worker. ");
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

    static class HelperReturnWorker extends Thread {
        Socket _requesterSocket;

        public HelperReturnWorker(Socket requesterSocket) {
            _requesterSocket = requesterSocket;
        }

        public void run() {

            try {
                Console.writeLine("Start helper returning");
                TcpMessenger messenger = new TcpMessenger(_requesterSocket);
                ServerInfo serverInfo = messenger.receiveServerInfo();
                String category = messenger.receiveString();
                _table.returnHelper(serverInfo, category);
                Console.writeLine("Helper on " + serverInfo + " is returned.\n");
            } catch (IOException e) {
                Console.writeLine("IO error in helper returning worker. ");
            } finally {
                try {
                    _requesterSocket.close();
                    Console.writeLine("Socket to client " + _requesterSocket.getInetAddress().getHostAddress() + ":" + _requesterSocket.getPort() + " is closed. ");
                } catch (IOException e) {
                    Console.writeLine("Socket to client " + _requesterSocket.getInetAddress().getHostAddress() + ":" + _requesterSocket.getPort() + " failed to close. ");
                }
            }


        }

    }


}
