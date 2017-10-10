package org.chiwooplatform.samples.support;

import java.util.zip.ZipInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;

import scala.Tuple2;

public class StreamFunction {

    private static InputStream openStream(final String path, final PortableDataStream pds) throws IOException {
        InputStream in = null;
        if (path.endsWith(".zip")) {
            final ZipInputStream zipIn = new ZipInputStream(pds.open());
            zipIn.getNextEntry(); // 이걸 왜 했지....?
            in = zipIn;
        }
        else if (path.endsWith(".bz2")) {
            in = new BZip2CompressorInputStream(pds.open());
        }
        else /* if (path.endsWith(".gz")) */ {
            in = new GzipCompressorInputStream(pds.open());
        }
        return in;
    }

    private static String readCompressedStream(final String path, final PortableDataStream pds)
            throws IOException, UnsupportedEncodingException {
        InputStream in = null;
        BufferedReader br = null;
        try {
            in = openStream(path, pds);
            StringBuffer buf = new StringBuffer();
            br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            String line = null;
            while ((line = br.readLine()) != null) {
                buf.append(line).append("\n");
            }
            return buf.toString();
        }
        finally {
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(in);
        }
    }

    /**
     * gz 파일을 압축 해제하여 Stream 을 읽어 들인다.
     * 
     * gz 파일을 한번에 압축 해제하고 그 결과를 컬렉션에 담는 처리가 좋은 구조 처럼 느껴지지 않는다. flatMap 과 같이 iterable 하게
     * 하는것이 좋겠다.
     */
    public static Function<Tuple2<String, PortableDataStream>, String> unzipToString = new Function<Tuple2<String, PortableDataStream>, String>() {
        private static final long serialVersionUID = 1L;

        @Override
        public String call(Tuple2<String, PortableDataStream> tuple) throws Exception {
            final String path = tuple._1();
            final PortableDataStream pds = tuple._2();
            return StreamFunction.readCompressedStream(path, pds);
        }
    };

    /*
     * @SuppressWarnings("serial") public static final <T> PairFunction<Tuple2<String,
     * PortableDataStream>, String, T> xxx = new PairFunction<Tuple2<String,
     * PortableDataStream>, String, T>() {
     * 
     * @Override public Tuple2<String, T> call(Tuple2<String, PortableDataStream> pds)
     * throws Exception { return (T)null; } };
     */

}
