/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.AssignReplicasToDirectoriesRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirectoriesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class AssignReplicasToDirectoriesRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<AssignReplicasToDirectoriesRequest> {
        private final AssignReplicasToDirectoriesRequestData data;

        public Builder(AssignReplicasToDirectoriesRequestData data) {
            super(ApiKeys.ASSIGN_REPLICAS_TO_DIRECTORIES);
            this.data = data;
        }

        @Override
        public AssignReplicasToDirectoriesRequest build(short version) {
            return new AssignReplicasToDirectoriesRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final AssignReplicasToDirectoriesRequestData data;

    public AssignReplicasToDirectoriesRequest(AssignReplicasToDirectoriesRequestData data, short version) {
        super(ApiKeys.ASSIGN_REPLICAS_TO_DIRECTORIES, version);
        this.data = data;
    }

    @Override
    public AssignReplicasToDirectoriesRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new AssignReplicasToDirectoriesResponse(new AssignReplicasToDirectoriesResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code()));
    }

    public static AssignReplicasToDirectoriesRequest parse(ByteBuffer buffer, short version) {
        return new AssignReplicasToDirectoriesRequest(new AssignReplicasToDirectoriesRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
