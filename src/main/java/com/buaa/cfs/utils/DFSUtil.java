/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.buaa.cfs.utils;

import com.google.common.primitives.SignedBytes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.UnsupportedEncodingException;
import java.security.SecureRandom;
import java.util.Random;


public class DFSUtil {
    public static final Log LOG = LogFactory.getLog(DFSUtil.class.getName());

    public static final byte[] EMPTY_BYTES = {};

    /** Compare two byte arrays by lexicographical order. */
    public static int compareBytes(byte[] left, byte[] right) {
        if (left == null) {
            left = EMPTY_BYTES;
        }
        if (right == null) {
            right = EMPTY_BYTES;
        }
        return SignedBytes.lexicographicalComparator().compare(left, right);
    }

    private DFSUtil() { /* Hidden constructor */ }

    private static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
            return new Random();
        }
    };

    private static final ThreadLocal<SecureRandom> SECURE_RANDOM = new ThreadLocal<SecureRandom>() {
        @Override
        protected SecureRandom initialValue() {
            return new SecureRandom();
        }
    };

    /** @return a pseudo random number generator. */
    public static Random getRandom() {
        return RANDOM.get();
    }

    /** @return a pseudo secure random number generator. */
    public static SecureRandom getSecureRandom() {
        return SECURE_RANDOM.get();
    }

    /** Shuffle the elements in the given array. */
    public static <T> T[] shuffle(final T[] array) {
        if (array != null && array.length > 0) {
            final Random random = getRandom();
            for (int n = array.length; n > 1; ) {
                final int randomIndex = random.nextInt(n);
                n--;
                if (n != randomIndex) {
                    final T tmp = array[randomIndex];
                    array[randomIndex] = array[n];
                    array[n] = tmp;
                }
            }
        }
        return array;
    }

    /**
     * Converts a byte array to a string using UTF8 encoding.
     */
    public static String bytes2String(byte[] bytes) {
        if (bytes != null) {
            return bytes2String(bytes, 0, bytes.length);
        }
        return null;
    }

    /**
     * Decode a specific range of bytes of the given byte array to a string using UTF8.
     *
     * @param bytes  The bytes to be decoded into characters
     * @param offset The index of the first byte to decode
     * @param length The number of bytes to decode
     *
     * @return The decoded string
     */
    public static String bytes2String(byte[] bytes, int offset, int length) {
        try {
            return new String(bytes, offset, length, "UTF8");
        } catch (UnsupportedEncodingException e) {
            assert false : "UTF8 encoding is not supported ";
        }
        return null;
    }

}
