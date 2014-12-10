/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.io.Directory;
import me.yuhuan.net.core.TcpMessenger;
import me.yuhuan.utilities.Console;

import java.net.Socket;
import java.util.ArrayList;

/**
 * Created by Yuhuan Jiang on 12/10/14.
 */
public class DemoProgram {



    public static void main(String[] args) throws Exception {

        while (true) {
            Console.clear();
            Console.writeLine("================== MINI GOOGLE ==================");
            Console.writeLine("What can I do for you: ");
            Console.writeLine("\t1. Index a document");
            Console.writeLine("\t2. Search for keywords");
            Console.writeLine("\t3. FOR DEMO ONLY: index all documents");


            int userChoice = Console.readInt();
            if (userChoice == 1) {
                Console.write("Please enter the directory that has the segmented files: ");
                String documentPath = Console.readLine();
                MiniGoogleLib.requestIndexing(documentPath);
            }
            else if (userChoice == 2) {
                Console.write("Keywords: ");
                String keywords = Console.readLine();
                String[] keywordArray = keywords.split("\\s+");
                MiniGoogleLib.requestSearching(keywordArray);
            }
            else if (userChoice == 3) {
                Console.writeLine("Indexing all documents");
                ArrayList<String> paths = Directory.getDirectories("working/parts");

                for (String path : paths) {
                    MiniGoogleLib.requestIndexing(path);
                    Thread.sleep(600);
                }

            }
            else {
                Console.writeLine("Please only choose from the options listed. ");
            }


            Console.writeLine("Hit ENTER to return to main menu. Press Ctrl+C to quit. ");
            String s = Console.readLine();

        }

    }

}
