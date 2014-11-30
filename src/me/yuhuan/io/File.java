/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

package me.yuhuan.io;

/**
 * Created by Yuhuan Jiang on 11/30/14.
 */
public class File {
    public static String extractFileNameFromPath(String path, Boolean withExtention) {
        java.io.File f = new java.io.File(path);
        String nameWithExtension = f.getName();
        if (!withExtention) {
            int dotPos = nameWithExtension.indexOf('.');
            nameWithExtension = nameWithExtension.substring(0, dotPos);
        }
        return nameWithExtension;
    }
}