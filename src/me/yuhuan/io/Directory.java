/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

package me.yuhuan.io;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by Yuhuan Jiang on 11/28/14.
 */
public class Directory {
    public static ArrayList<String> getFiles(String pathToDirectory) {
        ArrayList<String> result = new ArrayList<String>();
        File directory = new File(pathToDirectory);
        for (File file : directory.listFiles()) {
            if (file.isFile()) result.add(file.getAbsolutePath());
        }
        return result;
    }

    public static ArrayList<String> getDirectories(String pathToDirectory) {
        ArrayList<String> result = new ArrayList<String>();
        File directory = new File(pathToDirectory);
        for (File file : directory.listFiles()) {
            if (file.isDirectory()) result.add(file.getAbsolutePath());
        }
        return result;
    }

    public static Boolean createDirectory(String pathToDirectory) {
        return (new File(pathToDirectory)).mkdir();
    }
}
