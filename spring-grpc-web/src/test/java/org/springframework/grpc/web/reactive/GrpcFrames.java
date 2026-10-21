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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;

import com.google.protobuf.Message;

/**
 * Builds gRPC length-prefixed messages by hand, independently of the codecs under test.
 */
final class GrpcFrames {

	private GrpcFrames() {
	}

	/**
	 * Return the given message with its uncompressed prefix.
	 */
	static byte[] frame(Message message) {
		byte[] bytes = message.toByteArray();
		return concat(new byte[] { 0 }, ByteBuffer.allocate(4).putInt(bytes.length).array(), bytes);
	}

	static byte[] concat(byte[]... chunks) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (byte[] chunk : chunks) {
			out.writeBytes(chunk);
		}
		return out.toByteArray();
	}

	static DataBuffer buffer(byte[]... chunks) {
		return DefaultDataBufferFactory.sharedInstance.wrap(concat(chunks));
	}

	/**
	 * Read the bytes of the given buffer and release it.
	 */
	static byte[] bytes(DataBuffer buffer) {
		byte[] bytes = new byte[buffer.readableByteCount()];
		buffer.read(bytes);
		DataBufferUtils.release(buffer);
		return bytes;
	}

}
