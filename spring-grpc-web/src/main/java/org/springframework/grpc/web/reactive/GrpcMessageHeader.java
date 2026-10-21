/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.grpc.web.reactive;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import io.grpc.Status;

/**
 * The 5-byte prefix of every gRPC message on the wire: a compressed flag byte followed by
 * the length of the message as a 4-byte big-endian unsigned integer.
 *
 * @author Sharang Gupta
 * @see <a href=
 * "https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#requests">Length-Prefixed-Message</a>
 */
final class GrpcMessageHeader {

	/**
	 * The number of bytes in the header.
	 */
	static final int LENGTH = 5;

	private static final int UNCOMPRESSED = 0;

	private static final int COMPRESSED = 1;

	private GrpcMessageHeader() {
	}

	/**
	 * Write the header of an uncompressed message.
	 * @param messageLength the number of bytes in the message that follows the header
	 * @param outputStream the stream to write to
	 * @throws IOException if the stream cannot be written to
	 */
	static void write(int messageLength, OutputStream outputStream) throws IOException {
		outputStream.write(UNCOMPRESSED);
		outputStream.write(ByteBuffer.allocate(Integer.BYTES).putInt(messageLength).array());
	}

	/**
	 * Read the length of the message that follows the given header.
	 * @param header the {@link #LENGTH} bytes of the header
	 * @return the number of bytes in the message
	 * @throws GrpcDecodingException if the message is compressed, the header is malformed
	 * or the message length does not fit in an {@code int}
	 */
	static int readMessageLength(byte[] header) {
		int flag = Byte.toUnsignedInt(header[0]);
		if (flag == COMPRESSED) {
			throw new GrpcDecodingException(Status.Code.INTERNAL,
					"Cannot decode compressed gRPC message: compression is not supported");
		}
		if (flag != UNCOMPRESSED) {
			throw new GrpcDecodingException(Status.Code.INTERNAL,
					"Malformed gRPC message header: unknown compressed flag " + flag);
		}
		int messageLength = ByteBuffer.wrap(header, 1, Integer.BYTES).getInt();
		if (messageLength < 0) {
			throw new GrpcDecodingException(Status.Code.RESOURCE_EXHAUSTED,
					"gRPC message length " + Integer.toUnsignedLong(messageLength)
							+ " exceeds the largest supported length " + Integer.MAX_VALUE);
		}
		return messageLength;
	}

}
