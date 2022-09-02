/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** TODO: Add javadoc. */
public class HighLevelAPI {

    public static void main(String[] args) {
        HighLevelAPI.forSavepoint("inputPath")
                .forOperator(
                        Operation.withUid("uid")
                                .copy(
                                        new ValueStateDescriptor<String>("state1", String.class),
                                        Types.INT,
                                        Types.STRING)
                                .copy(
                                        new ValueStateDescriptor<String>("state2", String.class),
                                        Types.INT,
                                        Types.STRING)
                                .dropRemaining())
                .write("outputPath");
    }

    public static Builder forSavepoint(String savepointPath) {
        return new Builder(savepointPath);
    }

    public static class Builder {

        private final List<Operation> operations = new ArrayList<>();

        public Builder(String savepointPath) {}

        public Builder forOperator(Operation operation) {
            operations.add(operation);
            return this;
        }

        public void write(String savepointPath) {}
    }

    public interface Operation {

        void apply(SavepointReader savepointReader, SavepointWriter savepointWriter)
                throws IOException;

        static Builder withUid(String uid) {
            return new Builder(uid);
        }

        class Builder {
            private final String uid;

            private final List<StateReMapper<?, ?>> reMappers = new ArrayList<>();
            private final List<TypeInformation<?>> typeInfos = new ArrayList<>();
            private TypeInformation<?> keyTypeInfo;

            public Builder(String uid) {
                this.uid = uid;
            }

            <T, K> Builder copy(
                    ValueStateDescriptor<T> descriptor,
                    TypeInformation<K> keyTypeInfo,
                    TypeInformation<T> typeInformation) {
                this.keyTypeInfo = keyTypeInfo;
                this.reMappers.add(
                        new ValueStateReMapper(descriptor, descriptor, Function.identity()));
                this.typeInfos.add(typeInformation);
                return this;
            }

            Operation dropRemaining() {
                return new ValueStateOperation<>(uid, reMappers, typeInfos, keyTypeInfo);
            }
        }
    }

    static class ValueStateOperation<K> implements Operation {

        private final String uid;
        private final List<StateReMapper<?, ?>> reMappers;
        private final List<TypeInformation<?>> typeInfos;
        private final TypeInformation<K> keyTypeInfo;

        private ValueStateOperation(
                String uid,
                List<StateReMapper<?, ?>> reMappers,
                List<TypeInformation<?>> typeInfos,
                TypeInformation<K> keyTypeInfo) {
            this.uid = uid;
            this.reMappers = reMappers;
            this.typeInfos = typeInfos;
            this.keyTypeInfo = keyTypeInfo;
        }

        @Override
        public void apply(SavepointReader savepointReader, SavepointWriter savepointWriter)
                throws IOException {
            final DataStream<Tuple2<K, Tuple>> input =
                    savepointReader.readKeyedState(
                            uid,
                            new GenericKeyedStateReaderFunction<>(
                                    reMappers.toArray(new StateReMapper<?, ?>[0])),
                            keyTypeInfo,
                            new TupleTypeInfo<>(
                                    keyTypeInfo,
                                    new TupleTypeInfo<>(
                                            typeInfos.toArray(new TypeInformation<?>[0]))));

            final StateBootstrapTransformation<Tuple2<K, Tuple>> valueStateWriter =
                    OperatorTransformation.bootstrapWith(input)
                            .keyBy(t -> t.f0)
                            .transform(
                                    new GenericKeyedStateWriterFunction<K>(
                                            reMappers.toArray(new StateReMapper<?, ?>[0])));

            savepointWriter.removeOperator(uid).withOperator(uid, valueStateWriter);
        }
    }

    public interface StateReMapper<I, T> extends Serializable {

        void initRead(RuntimeContext runtimeContext);

        void initWrite(RuntimeContext runtimeContext);

        T get() throws IOException;

        void remap(I input) throws IOException;
    }

    public static class ValueStateReMapper<ORIGINAL_T, NEW_T>
            implements StateReMapper<ORIGINAL_T, ORIGINAL_T> {

        private static final long serialVersionUID = 1L;

        private final ValueStateDescriptor<ORIGINAL_T> readDescriptor;
        private final ValueStateDescriptor<NEW_T> writeDescriptor;
        private final Function<ORIGINAL_T, NEW_T> converter;

        private transient ValueState<ORIGINAL_T> state;
        private transient ValueState<NEW_T> newState;

        public ValueStateReMapper(
                ValueStateDescriptor<ORIGINAL_T> readDescriptor,
                ValueStateDescriptor<NEW_T> writeDescriptor,
                Function<ORIGINAL_T, NEW_T> converter) {
            this.readDescriptor = readDescriptor;
            this.writeDescriptor = writeDescriptor;
            this.converter = converter;
        }

        @Override
        public void initRead(RuntimeContext runtimeContext) {
            this.state = runtimeContext.getState(readDescriptor);
        }

        @Override
        public void initWrite(RuntimeContext runtimeContext) {
            this.newState = runtimeContext.getState(writeDescriptor);
        }

        @Override
        public ORIGINAL_T get() throws IOException {
            return state.value();
        }

        @Override
        public void remap(ORIGINAL_T input) throws IOException {
            newState.update(converter.apply(input));
        }
    }

    private static class GenericKeyedStateReaderFunction<K>
            extends KeyedStateReaderFunction<K, Tuple2<K, Tuple>> {

        private final StateReMapper<?, ?>[] reMappers;

        private GenericKeyedStateReaderFunction(StateReMapper<?, ?>[] reMappers) {
            this.reMappers = reMappers;
        }

        @Override
        public void open(Configuration configuration) {
            for (StateReMapper<?, ?> reMapper : reMappers) {
                reMapper.initRead(getRuntimeContext());
            }
        }

        @Override
        public void readKey(K k, Context context, Collector<Tuple2<K, Tuple>> collector)
                throws Exception {
            Tuple result = Tuple.newInstance(reMappers.length);
            for (int i = 0; i < reMappers.length; i++) {
                result.setField(reMappers[i].get(), i);
            }
            collector.collect(Tuple2.of(k, result));
        }
    }

    public static class GenericKeyedStateWriterFunction<K>
            extends KeyedStateBootstrapFunction<K, Tuple2<K, Tuple>> {

        private final StateReMapper<?, ?>[] reMappers;

        public GenericKeyedStateWriterFunction(StateReMapper<?, ?>... reMappers) {
            this.reMappers = reMappers;
        }

        @Override
        public void open(Configuration parameters) {
            for (StateReMapper<?, ?> reMapper : reMappers) {
                reMapper.initWrite(getRuntimeContext());
            }
        }

        @Override
        public void processElement(
                Tuple2<K, Tuple> value,
                KeyedStateBootstrapFunction<K, Tuple2<K, Tuple>>.Context ctx)
                throws Exception {
            for (int i = 0; i < reMappers.length; i++) {
                reMappers[i].remap(value.f1.getField(i));
            }
        }
    }
}
