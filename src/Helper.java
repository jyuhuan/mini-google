/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.net.Utilities;
import me.yuhuan.net.core.ServerInfo;
import me.yuhuan.net.core.TcpMessenger;
import me.yuhuan.utilities.Console;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Yuhuan Jiang on 11/27/14.
 */
public class Helper {

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
                TcpMessenger messenger = new TcpMessenger(_clientSocket);

                // Obtain the path to the file segment.
                String pathToSeg = messenger.receiveString();

                // Obtain the transaction ID.
                int transactionId = messenger.receiveInt();



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
}
