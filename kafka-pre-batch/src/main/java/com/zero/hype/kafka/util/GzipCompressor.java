package com.zero.hype.kafka.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * GzipCompressor provides GZIP compression and decompression functionality for byte arrays.
 * This utility class is used to compress batched messages before sending them to Kafka
 * and to decompress them when consuming.
 *
 * Key features:
 * - GZIP compression and decompression of byte arrays
 * - Automatic detection of GZIP-compressed data
 * - Efficient memory usage with streaming compression
 * - Thread-safe operation
 *
 * Usage example:
 * <pre>
 * GzipCompressor compressor = new GzipCompressor();
 * byte[] compressed = compressor.compress(data);
 * byte[] decompressed = compressor.decompress(compressed);
 * </pre>
 */
public class GzipCompressor {

    /**
     * Compresses a byte array using GZIP compression.
     * If the input is null, returns null. If compression fails,
     * throws an IOException with details.
     *
     * @param data The byte array to compress
     * @return The compressed byte array, or null if input is null
     * @throws IOException if compression fails
     */
    public byte[] compress(byte[] data) throws IOException {
        if (data != null) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
                    gzos.write(data);
                }
                return baos.toByteArray();
            } catch (IOException ioException) {
                throw new IOException(String.format("Error compressing data [%s]", ioException.getMessage()), ioException);
            }
        }
        return data;
    }

    /**
     * Decompresses a GZIP-compressed byte array.
     * If the input is null, empty, or not GZIP-compressed, returns the input as-is.
     * If decompression fails, throws an IOException with details.
     *
     * @param data The compressed byte array to decompress
     * @return The decompressed byte array, or the input if not compressed
     * @throws IOException if decompression fails
     */
    public byte[] decompress(byte[] data) throws IOException {
        if (data != null && data.length > 0 && isCompressed(data)) {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
                 GZIPInputStream gzis = new GZIPInputStream(bais);
                 ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[4096];
                int len;
                while ((len = gzis.read(buffer)) > 0) {
                    baos.write(buffer, 0, len);
                }
                return baos.toByteArray();
            } catch (IOException e) {
                throw new IOException(String.format("Error decompressing data [%s]", e.getMessage()), e);
            }
        }
        return data;
    }

    /**
     * Checks if a byte array is GZIP-compressed by examining its magic number.
     * GZIP files start with the magic number 0x1f8b.
     *
     * @param data The byte array to check
     * @return true if the data appears to be GZIP-compressed, false otherwise
     */
    public boolean isCompressed(byte[] data) {
        return (data[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) && 
               (data[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
    }
}
