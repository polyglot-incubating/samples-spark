package org.chiwooplatform.samples.support;

import java.util.Iterator;

import org.apache.spark.api.java.function.Function2;

public final class SkipRowFunction<T, R> implements Function2<Integer, Iterator<T>, Iterator<R>> {

    private static final long serialVersionUID = 1L;

    private final int skipRownum;

    public SkipRowFunction(int _skipRownum) {
        this.skipRownum = _skipRownum;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<R> call(Integer idx, Iterator<T> val) throws Exception {
        if (idx < skipRownum) {
            val.next();
        }
        return (Iterator<R>) val;
    }

}