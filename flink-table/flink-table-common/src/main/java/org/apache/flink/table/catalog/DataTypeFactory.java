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
package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;

/**
 * Factory for creating fully resolved data types that can be used for planning.
 *
 * <p>The factory is useful for types that cannot be created with one of the static methods in
 * {@link DataTypes}) because they require access to configuration or catalog.
 */
@PublicEvolving
public interface DataTypeFactory {

    /**
     * Creates a type out of an {@link AbstractDataType}.
     *
     * <p>If the given type is already a {@link DataType}, the factory will return it unmodified. In
     * case of {@link UnresolvedDataType}, the factory will resolve it to a {@link DataType}.
     */
    DataType createDataType(AbstractDataType<?> abstractDataType);

    /**
     * Creates a type by a fully or partially defined name.
     *
     * <p>The factory will parse and resolve the name of a type to a {@link DataType}. This includes
     * both built-in types and user-defined types (see {@link DistinctType} and {@link
     * StructuredType}).
     */
    DataType createDataType(String typeString);

    /**
     * Creates a type by a fully or partially defined identifier.
     *
     * <p>The factory will parse and resolve the name of a type to a {@link DataType}. This includes
     * both built-in types and user-defined types (see {@link DistinctType} and {@link
     * StructuredType}).
     */
    DataType createDataType(UnresolvedIdentifier identifier);

    /**
     * Creates a type by analyzing the given class.
     *
     * <p>It does this by using Java reflection which can be supported by {@link DataTypeHint}
     * annotations for nested, structured types.
     *
     * <p>It will throw an {@link ValidationException} in cases where the reflective extraction
     * needs more information or simply fails.
     *
     * <p>See {@link DataTypes#of(Class)} for further examples.
     */
    <T> DataType createDataType(Class<T> clazz);

    /**
     * Creates a type for the given {@link TypeInformation}.
     *
     * <p>{@link DataType} is richer than {@link TypeInformation} as it also includes details about
     * the {@link LogicalType}. Therefore, some details will be added implicitly during the
     * conversion. The mapping to data type happens on a best effort basis. If no data type is
     * suitable, the type information is interpreted as {@link DataTypes#RAW(Class,
     * TypeSerializer)}.
     *
     * <p>See {@link TypeInfoDataTypeConverter} for more information.
     */
    <T> DataType createDataType(TypeInformation<T> typeInfo);

    /**
     * Creates a RAW type for the given class in cases where no serializer is known and a generic
     * serializer should be used. The factory will create {@link DataTypes#RAW(Class,
     * TypeSerializer)} with Flink's default RAW serializer that is automatically configured.
     *
     * <p>Note: This type is a black box within the table ecosystem and is only deserialized at the
     * edges of the API.
     */
    <T> DataType createRawDataType(Class<T> clazz);

    /**
     * Creates a RAW type for the given {@link TypeInformation}. Since type information does not
     * contain a {@link TypeSerializer} yet. The serializer will be generated by considering the
     * current configuration.
     *
     * <p>Note: This type is a black box within the table ecosystem and is only deserialized at the
     * edges of the API.
     */
    <T> DataType createRawDataType(TypeInformation<T> typeInfo);

    // --------------------------------------------------------------------------------------------
    // LogicalType creation
    // --------------------------------------------------------------------------------------------

    /**
     * Creates a {@link LogicalType} by a fully or partially defined name.
     *
     * <p>The factory will parse and resolve the name of a type to a {@link LogicalType}. This
     * includes both built-in types and user-defined types (see {@link DistinctType} and {@link
     * StructuredType}).
     */
    LogicalType createLogicalType(String typeString);

    /**
     * Creates a {@link LogicalType} from an {@link UnresolvedIdentifier} for resolving user-defined
     * types (see {@link DistinctType} and {@link StructuredType}).
     */
    LogicalType createLogicalType(UnresolvedIdentifier identifier);
}
