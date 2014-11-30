/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

package me.yuhuan.io;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by Yuhuan Jiang on 10/26/14.
 */
public class TextFile {
    public static void write(String path, String[] lines) throws UnsupportedEncodingException, FileNotFoundException {
        PrintWriter writer = new PrintWriter(path, "UTF-8");
        for (String line : lines) {
            writer.println(line);
        }
        writer.close();
    }

    public static void write(String path, ArrayList<String> lines) throws UnsupportedEncodingException, FileNotFoundException {
        PrintWriter writer = new PrintWriter(path, "UTF-8");
        for (String line : lines) {
            writer.println(line);
        }
        writer.close();
    }

    public static String[] read(String path) throws IOException {
        ArrayList<String> lines = new ArrayList<String>();
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(new java.io.File(path)), "UTF8"));
        String line;
        while ((line = in.readLine()) != null) {
            lines.add(line);
        }
        in.close();
        return lines.toArray(new String[lines.size()]);
    }

    public static boolean exists(String path) throws IOException {
        java.io.File f = new java.io.File(path);
        return (f.exists() && !f.isDirectory());
    }

    public static void createEmptyFile(String path) throws IOException {
        write(path, new String[] {});
    }
}
