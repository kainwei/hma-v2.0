package hma.util;

/**
 * Created with IntelliJ IDEA.
 * User: synckey
 * Date: 13-9-10
 * Time: 上午11:26
 * To change this template use File | Settings | File Templates.
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public class Pair<T1, T2> {
    private final T1 left;
    private final T2 right;

    public Pair(T1 left, T2 right) {
        this.left = left;
        this.right = right;
    }

    public T1 getLeft() {
        return left;
    }

    public T2 getRight() {
        return right;
    }

    @Override
    public final int hashCode() {
        int hashCode = 31 + (left == null ? 0 : left.hashCode());
        return 31 * hashCode + (right == null ? 0 : right.hashCode());
    }

    @Override
    public String toString() {
        return "(" + left + "," + right + ")";
    }

    public static <X, Y> Pair<X, Y> create(X x, Y y) {
        return new Pair<X, Y>(x, y);
    }

    public static void main(String args[]) {
        Pair<Integer, StringHelper> p1 = new Pair(1, "test");
        Pair<Integer, Double> p2 = Pair.create(1, 2.2);
        System.out.println(p1.getLeft());
        System.out.println(p1.getRight());
        System.out.println(p2.getLeft());
        System.out.println(p2.getRight());
        System.out.println(p1.toString());
        System.out.println(p2.toString());
        System.out.println(p1.hashCode());
        System.out.println(p2.hashCode());

    }
}
