/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

package me.yuhuan.net.core;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Yuhuan Jiang on 10/18/14.
 */
public class ServerInfo {
    public int[] ipAddress;
    public int portNumber;

    public static ServerInfo createFakeServer() {
        return new ServerInfo("-1.-1.-1.-1", -1);
    }

    public String IPAddressString() {
        return ipAddress[0] + "." + ipAddress[1] + "." + ipAddress[2] + "." + ipAddress[3];
    }

    public InetAddress inetAddress() throws UnknownHostException {
        return InetAddress.getByName(this.IPAddressString());
    }

    public ServerInfo(String ipAddressString, int portNumber) {
        String[] components = ipAddressString.split("\\.");
        ipAddress = new int[4];
        for (int i = 0; i < 4; i++) {
            ipAddress[i] = Integer.parseInt(components[i]);
        }
        this.portNumber = portNumber;
    }

    public ServerInfo(int[] IPAddress, int portNumber) {
        this.ipAddress = IPAddress;
        this.portNumber = portNumber;
    }

    @Override
    public int hashCode() {
        int hashCode = 17;
        hashCode = hashCode * 23 + ipAddress[0];
        hashCode = hashCode * 23 + ipAddress[1];
        hashCode = hashCode * 23 + ipAddress[2];
        hashCode = hashCode * 23 + ipAddress[3];
        hashCode = hashCode * 23 + portNumber;
        return hashCode;
    }

    @Override
    public boolean equals(Object other) {
        ServerInfo theOther = (ServerInfo)other;
        return theOther.ipAddress[0] == this.ipAddress[0] &&
                theOther.ipAddress[1] == this.ipAddress[1] &&
                theOther.ipAddress[2] == this.ipAddress[2] &&
                theOther.ipAddress[3] == this.ipAddress[3] &&
                theOther.portNumber == this.portNumber;
    }

    @Override
    public String toString() {
        return "(" + IPAddressString() + ", " + portNumber + ")";
    }


}
