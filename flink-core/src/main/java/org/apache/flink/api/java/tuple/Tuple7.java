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
package org.apache.flink.api.java.tuple;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.StringUtils;

/**
 * A tuple with 7 fields. Tuples are strongly typed; each field may be of a separate type. The
 * fields of the tuple can be accessed directly as public fields (f0, f1, ...) or via their position
 * through the {@link #getField(int)} method. The tuple field positions start at zero.
 *
 * <p>Tuples are mutable types, meaning that their fields can be re-assigned. This allows functions
 * that work with Tuples to reuse objects in order to reduce pressure on the garbage collector.
 *
 * <p>Warning: If you subclass Tuple7, then be sure to either
 *
 * <ul>
 *   <li>not add any new fields, or
 *   <li>make it a POJO, and always declare the element type of your DataStreams/DataSets to your
 *       descendant type. (That is, if you have a "class Foo extends Tuple7", then don't use
 *       instances of Foo in a DataStream&lt;Tuple7&gt; / DataSet&lt;Tuple7&gt;, but declare it as
 *       DataStream&lt;Foo&gt; / DataSet&lt;Foo&gt;.)
 * </ul>
 *
 * @see Tuple
 * @param <T0> The type of field 0
 * @param <T1> The type of field 1
 * @param <T2> The type of field 2
 * @param <T3> The type of field 3
 * @param <T4> The type of field 4
 * @param <T5> The type of field 5
 * @param <T6> The type of field 6
 */
@Public
public class Tuple7<T0, T1, T2, T3, T4, T5, T6> extends Tuple {

    private static final long serialVersionUID = 1L;

    /** Field 0 of the tuple. */
    public T0 f0;
    /** Field 1 of the tuple. */
    public T1 f1;
    /** Field 2 of the tuple. */
    public T2 f2;
    /** Field 3 of the tuple. */
    public T3 f3;
    /** Field 4 of the tuple. */
    public T4 f4;
    /** Field 5 of the tuple. */
    public T5 f5;
    /** Field 6 of the tuple. */
    public T6 f6;

    /** Creates a new tuple where all fields are null. */
    public Tuple7() {}

    /**
     * Creates a new tuple and assigns the given values to the tuple's fields.
     *
     * @param f0 The value for field 0
     * @param f1 The value for field 1
     * @param f2 The value for field 2
     * @param f3 The value for field 3
     * @param f4 The value for field 4
     * @param f5 The value for field 5
     * @param f6 The value for field 6
     */
    public Tuple7(T0 f0, T1 f1, T2 f2, T3 f3, T4 f4, T5 f5, T6 f6) {
        this.f0 = f0;
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
        this.f5 = f5;
        this.f6 = f6;
    }

    @Override
    public int getArity() {
        return 7;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getField(int pos) {
        switch (pos) {
            case 0:
                return (T) this.f0;
            case 1:
                return (T) this.f1;
            case 2:
                return (T) this.f2;
            case 3:
                return (T) this.f3;
            case 4:
                return (T) this.f4;
            case 5:
                return (T) this.f5;
            case 6:
                return (T) this.f6;
            default:
                throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void setField(T value, int pos) {
        switch (pos) {
            case 0:
                this.f0 = (T0) value;
                break;
            case 1:
                this.f1 = (T1) value;
                break;
            case 2:
                this.f2 = (T2) value;
                break;
            case 3:
                this.f3 = (T3) value;
                break;
            case 4:
                this.f4 = (T4) value;
                break;
            case 5:
                this.f5 = (T5) value;
                break;
            case 6:
                this.f6 = (T6) value;
                break;
            default:
                throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    /**
     * Sets new values to all fields of the tuple.
     *
     * @param f0 The value for field 0
     * @param f1 The value for field 1
     * @param f2 The value for field 2
     * @param f3 The value for field 3
     * @param f4 The value for field 4
     * @param f5 The value for field 5
     * @param f6 The value for field 6
     */
    public void setFields(T0 f0, T1 f1, T2 f2, T3 f3, T4 f4, T5 f5, T6 f6) {
        this.f0 = f0;
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
        this.f5 = f5;
        this.f6 = f6;
    }

    // -------------------------------------------------------------------------------------------------
    // standard utilities
    // -------------------------------------------------------------------------------------------------

    /**
     * Creates a string representation of the tuple in the form (f0, f1, f2, f3, f4, f5, f6), where
     * the individual fields are the value returned by calling {@link Object#toString} on that
     * field.
     *
     * @return The string representation of the tuple.
     */
    @Override
    public String toString() {
        return "("
                + StringUtils.arrayAwareToString(this.f0)
                + ","
                + StringUtils.arrayAwareToString(this.f1)
                + ","
                + StringUtils.arrayAwareToString(this.f2)
                + ","
                + StringUtils.arrayAwareToString(this.f3)
                + ","
                + StringUtils.arrayAwareToString(this.f4)
                + ","
                + StringUtils.arrayAwareToString(this.f5)
                + ","
                + StringUtils.arrayAwareToString(this.f6)
                + ")";
    }

    /**
     * Deep equality for tuples by calling equals() on the tuple members.
     *
     * @param o the object checked for equality
     * @return true if this is equal to o.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Tuple7)) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        Tuple7 tuple = (Tuple7) o;
        if (f0 != null ? !f0.equals(tuple.f0) : tuple.f0 != null) {
            return false;
        }
        if (f1 != null ? !f1.equals(tuple.f1) : tuple.f1 != null) {
            return false;
        }
        if (f2 != null ? !f2.equals(tuple.f2) : tuple.f2 != null) {
            return false;
        }
        if (f3 != null ? !f3.equals(tuple.f3) : tuple.f3 != null) {
            return false;
        }
        if (f4 != null ? !f4.equals(tuple.f4) : tuple.f4 != null) {
            return false;
        }
        if (f5 != null ? !f5.equals(tuple.f5) : tuple.f5 != null) {
            return false;
        }
        if (f6 != null ? !f6.equals(tuple.f6) : tuple.f6 != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = f0 != null ? f0.hashCode() : 0;
        result = 31 * result + (f1 != null ? f1.hashCode() : 0);
        result = 31 * result + (f2 != null ? f2.hashCode() : 0);
        result = 31 * result + (f3 != null ? f3.hashCode() : 0);
        result = 31 * result + (f4 != null ? f4.hashCode() : 0);
        result = 31 * result + (f5 != null ? f5.hashCode() : 0);
        result = 31 * result + (f6 != null ? f6.hashCode() : 0);
        return result;
    }

    /**
     * Shallow tuple copy.
     *
     * @return A new Tuple with the same fields as this.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Tuple7<T0, T1, T2, T3, T4, T5, T6> copy() {
        return new Tuple7<>(this.f0, this.f1, this.f2, this.f3, this.f4, this.f5, this.f6);
    }

    /**
     * Creates a new tuple and assigns the given values to the tuple's fields. This is more
     * convenient than using the constructor, because the compiler can infer the generic type
     * arguments implicitly. For example: {@code Tuple3.of(n, x, s)} instead of {@code new
     * Tuple3<Integer, Double, String>(n, x, s)}
     */
    public static <T0, T1, T2, T3, T4, T5, T6> Tuple7<T0, T1, T2, T3, T4, T5, T6> of(
            T0 f0, T1 f1, T2 f2, T3 f3, T4 f4, T5 f5, T6 f6) {
        return new Tuple7<>(f0, f1, f2, f3, f4, f5, f6);
    }
}
