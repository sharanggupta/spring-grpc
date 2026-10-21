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
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.http.MediaType;
import org.springframework.http.codec.protobuf.ProtobufEncoder;
import org.springframework.util.MimeType;

import com.google.protobuf.Message;

/**
 * An {@code Encoder} that writes Protobuf {@link Message Messages} to a gRPC response
 * body, prefixing each message with an uncompressed flag and its length.
 * <p>
 * Messages are only encoded for the {@link GrpcMediaTypes gRPC media types}. Every
 * message is framed the same way whether it is written alone or as part of a stream, so
 * all of those media types are streaming media types and each message is flushed as soon
 * as it is written.
 *
 * @author Dave Syer
 * @author Sharang Gupta
 * @since 1.1.2
 */
public class GrpcEncoder extends ProtobufEncoder {

	/**
	 * Create a new {@code GrpcEncoder}.
	 */
	public GrpcEncoder() {
		setMimeTypes(List.copyOf(GrpcMediaTypes.ALL));
	}

	@Override
	protected boolean supportsMimeType(@Nullable MimeType mimeType) {
		return GrpcMediaTypes.isGrpc(mimeType);
	}

	@Override
	public List<MediaType> getStreamingMediaTypes() {
		return GrpcMediaTypes.ALL;
	}

	/**
	 * Write the message with its prefix. The prefix delimits every gRPC message, so the
	 * {@code delimited} flag that the Protobuf encoder sets for streams is ignored.
	 */
	@Override
	protected void writeMessage(Message message, boolean delimited, OutputStream outputStream) throws IOException {
		GrpcMessageHeader.write(message.getSerializedSize(), outputStream);
		message.writeTo(outputStream);
	}

}
