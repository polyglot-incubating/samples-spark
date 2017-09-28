package org.chiwooplatform.samples;

public enum CompressionType {
// @formatter:off
    BROTLI("br"),
    BZIP2("bzip2"),
    GZIP("gz"),
    PACK200("pack200"),
    XZ("xz"),
    LZMA("lzma"),
    SNAPPY_FRAMED("snappy-framed"),
    SNAPPY_RAW("snappy-raw"),
    Z("z"),
    DEFLATE("deflate"),
    LZ4_BLOCK("lz4-block"),
    LZ4_FRAMED("lz4-framed") 
    ;
// @formatter:on

    private final String compressType;

    CompressionType(String _compressType) {
        compressType = _compressType;
    }

    public String compressType() {
        return this.compressType;
    }
}
