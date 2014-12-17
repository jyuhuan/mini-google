/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

package me.yuhuan.utilities;

/**
 * Created by Yuhuan Jiang on 10/26/14.
 */
public class UidGenerator {
    private static Integer _id;
    public synchronized static int next() {
        if (_id == null) {
            _id = 0;
        }
        else {
            _id++;
        }
        return _id;
    }

}
