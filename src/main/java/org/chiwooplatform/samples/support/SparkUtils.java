package org.chiwooplatform.samples.support;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public final class SparkUtils {

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    public static String getPath(String path) throws IOException {
        File f = new File(new File(".").getCanonicalPath() + "/");
        final String filepath = f.getCanonicalPath();
        assert f.exists() : "File not found! '" + filepath + "'.";
        return filepath;
    }

    public static String resourcePath(String path) throws IOException {
        File f = new File(new File(".").getCanonicalPath() + "/src/main/resources/" + path);
        final String filepath = f.getCanonicalPath();
        assert f.exists() : "File not found! '" + filepath + "'.";
        return filepath;
    }

    public static Column column(final String columnName) {
        return org.apache.spark.sql.functions.col(columnName);
    }

    private static final BiFunction<String, DataType, StructField> structField = (name, type) -> DataTypes
            .createStructField(name, type, true);

    public static StructType stringSchema(final String columnName) {
        return SparkUtils.buildSchema(new String[] { columnName }, new DataType[] { DataTypes.StringType });
    }

    public static StructType buildSchema(final String[] tuples) {
        List<StructField> fields = new ArrayList<>();
        for (String column : tuples) {
            fields.add(structField.apply(column, DataTypes.StringType));
        }
        final StructType structType = DataTypes.createStructType(fields);
        return structType;
    }

    public static StructType buildSchema(final String[] tuples, final DataType[] types) {
        List<StructField> fields = new ArrayList<>();
        IntStream.range(0, tuples.length).mapToObj(i -> structField.apply(tuples[i], types[i])).forEach(f -> {
            // System.out.printf("\n %s : %s : %s", f.name(), f.dataType(),
            // f.getClass().getName());
            fields.add(f);
        });
        final StructType structType = DataTypes.createStructType(fields);
        return structType;
    }

    public static final void log(List<? extends Object> data) {
        for (Object value : data) {
            if (value instanceof Tuple2) {
                Tuple2<?, ?> tuple = (Tuple2<?, ?>) value;
                System.out.printf("%s: %s  \n", tuple._1(), tuple._2());
            }
            else {
                System.out.println(value);
            }
        }
    }

    public static List<File> decompressGzip(final File file) throws CompressorException, ArchiveException, IOException {
        final String decompressFilePath = file.getPath();
        File outputFilePath = new File(decompressFilePath);
        if (!outputFilePath.exists()) {
            ///
        }
        final byte[] gzipBytes = gzipToBytes(file);

        List<File> files = new LinkedList<>();
        ArchiveInputStream arin = null;
        try {
            arin = new ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.TAR,
                    new BufferedInputStream(new ByteArrayInputStream(gzipBytes)));
            ArchiveEntry entry = null;
            while ((entry = arin.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    File outdir = new File(outputFilePath, entry.getName());
                    outdir.mkdirs();
                    files.add(outdir);
                }
                else {
                    File outFile = new File(outputFilePath, entry.getName());
                    if (!outFile.getParentFile().exists()) {
                        outFile.getParentFile().mkdirs();
                    }
                    files.add(outFile);
                    OutputStream os = null;
                    try {
                        os = new BufferedOutputStream(new FileOutputStream(outFile));
                        IOUtils.copy(arin, os);
                    }
                    finally {
                        if (os != null)
                            os.close();
                    }
                }
            }

        }
        finally {
            IOUtils.closeQuietly(arin);
        }
        return null;
    }

    public static byte[] gzipToBytes(final File file) throws CompressorException, IOException {

        ByteArrayOutputStream baos = null;
        CompressorInputStream cin = null;
        try {
            cin = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP,
                    new BufferedInputStream(new FileInputStream(file)));

            baos = new ByteArrayOutputStream();
            IOUtils.copy(cin, baos);
            return baos.toByteArray();
        }
        finally {
            IOUtils.closeQuietly(baos);
            IOUtils.closeQuietly(cin);
        }
    }

}
