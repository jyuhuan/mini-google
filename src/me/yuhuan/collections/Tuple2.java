/*
 * This work is licensed under a Creative Commons Attribution-NonCommercial-NoDerivatives 4.0
 * International License (http://creativecommons.org/licenses/by-nc-nd/4.0/).
 */

package me.yuhuan.collections;

/**
 * Created by Yuhuan Jiang on 10/18/14.
 */
public class Tuple2<T1, T2> {

    public T1 item1;
    public T2 item2;

    public Tuple2(T1 item1, T2 item2) {
        this.item1 = item1;
        this.item2 = item2;
    }

    @Override
    public int hashCode() {
        int hashCode = 17;
        hashCode = hashCode * 23 + item1.hashCode();
        hashCode = hashCode * 23 + item2.hashCode();
        return hashCode;
    }

    @Override
    public boolean equals(Object that) {
        Tuple2<T1, T2> thatTuple = (Tuple2<T1, T2>)that;
        return this.item1.equals(thatTuple.item1) &&
                this.item2.equals(thatTuple.item2);
    }

    @Override
    public String toString() {
        return "(" + item1 + ", " + item2 + ")";
    }
}