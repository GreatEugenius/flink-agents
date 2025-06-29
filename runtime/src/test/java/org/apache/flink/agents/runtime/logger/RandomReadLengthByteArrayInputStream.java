/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.agents.runtime.logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Random;

/**
 * NOTE: This source code was copied from the <a
 * href="https://github.com/apache/flink-statefun">flink-statefun</a>.
 *
 * <p>A {@link ByteArrayInputStream} that reads a random number of bytes per read (at least 1) up to
 * the requested amount, and always returns 0 on {@link InputStream#available()}.
 *
 * <p>We use this input stream in our tests to mimic behaviour of "real-life" input streams, while
 * still adhering to the contracts of the {@link InputStream} methods:
 *
 * <ul>
 *   <li>For {@link InputStream#read(byte[])} and {@link InputStream#read()}: read methods always
 *       blocks until at least 1 byte is available from the stream; it always at least reads 1 byte.
 *   <li>For {@link InputStream#available()}: always return 0, to imply that there are no bytes
 *       immediately available from the stream, and the next read will block.
 * </ul>
 */
final class RandomReadLengthByteArrayInputStream extends ByteArrayInputStream {

    private static final Random RANDOM = new Random();

    RandomReadLengthByteArrayInputStream(byte[] byteBuffer) {
        super(byteBuffer);
    }

    @Override
    public int read(byte[] b, int off, int len) {
        final int randomNumBytesToRead = RANDOM.nextInt(len) + 1;
        return super.read(b, off, randomNumBytesToRead);
    }

    @Override
    public synchronized int available() {
        return 0;
    }
}
