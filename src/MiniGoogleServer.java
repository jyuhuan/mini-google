/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.net.core.TcpMessenger;
import me.yuhuan.utilities.Console;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Yuhuan Jiang on 11/24/14.
 */
public class MiniGoogleServer {

    // Configuration of the server



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

    static class IndexingMaster extends Thread {
        Socket _clientSocket;

        public IndexingMaster(Socket clientSocket) {
            _clientSocket = clientSocket;
        }

        public void run() {
            try {
                TcpMessenger messenger = new TcpMessenger(_clientSocket);
                String pathToSegments = messenger.receiveString();
                int transactionId = messenger.receiveInt();

                // Phase I: Mapping
                // 1. Count number of files, N.
                // 2. Apply for N mapping helpers from the name server.
                // 3. Request indexing mapping on each helpers for each segment.


                // Phrase II: Reducing
                // TODO:

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
