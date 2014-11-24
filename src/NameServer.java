/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

import me.yuhuan.collections.MultiValueHashTable;
import me.yuhuan.collections.exceptions.MultiValueHashTableLookupFailureException;
import me.yuhuan.net.core.ServerInfo;
import me.yuhuan.utility.Console;

/**
 * Created by Yuhuan Jiang on 11/24/14.
 */
public class NameServer {

    static MultiValueHashTable<String, ServerInfo> _map;

    public static void main(String[] args) {
        _map = new MultiValueHashTable<String, ServerInfo>();

        _map.add("AA", new ServerInfo("1.2.3.4", 1234));
        _map.add("AB", new ServerInfo("1.2.4.5", 1245));
        _map.add("AA", new ServerInfo("1.2.3.4", 1234));
        _map.add("AA", new ServerInfo("1.3.7.8", 1378));

        ServerInfo getAA;
        for (int i = 0; i < 5; i++) {
            try {
                getAA = _map.get("AA");
            }
            catch (MultiValueHashTableLookupFailureException e) {
                Console.writeLine(e.getMessage());
            }
        }


    }


}
