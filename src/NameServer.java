/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.collections.MultiValueHashTable;
import me.yuhuan.collections.Tuple2;
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

/**
 * Created by Yuhuan Jiang on 11/24/14.
 */
public class NameServer {
    static MultiValueHashTable<String, ServerInfo> _map;

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
        categories.add("c1");
        categories.add("c2");
        categories.add("c3");
        categories.add("c4");
        categories.add("c5");
        return categories;
    }

    public static void main(String[] args) throws IOException {

        // Every time the name server starts, the map is created as follows:
        //    s1 => (Empty list)
        //    s2 => (Empty list)
        //    ...
        //    z1 => (Empty list)

        // Generate a categories
        ArrayList<String> categories = generateSimpleCategories();

        // Create the table with the categories as keys, and value being empty.
        _map = new MultiValueHashTable<String, ServerInfo>(categories);

        // A socket that listens to incoming requests on a system-allocated port number.
        ServerSocket serverSocket = new ServerSocket(12345); // TODO: change to 0

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
                ServerInfo serverInfo = messenger.receiveServerInfo();

                String categoryAssigned = MiniGoogleUtilities.getNextCategoryToRegisterForInTable(_map);
                _map.add(categoryAssigned, serverInfo);

                messenger.sendString(categoryAssigned);
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

}
