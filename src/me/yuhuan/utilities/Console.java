/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

package me.yuhuan.utilities;

/**
 * Created by Yuhuan Jiang on 10/16/14.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class Console {
    public static void write(String line) {
        System.out.print(line);
    }

    public static void writeLine(String line) {
        System.out.println(line);
    }

    public static String readLine() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        return reader.readLine();
    }

    public static int readInt() throws IOException {
        return Integer.parseInt(readLine());
    }

    public static void clear() throws IOException {
        Runtime.getRuntime().exec("clear");
    }
}
