package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class JNILoaderTest {

    @Test
    public void testResourcePath() {

        assertEquals("/lib/x86_64-linux-gnu/libtb_jniclient.so", JNILoader
                .getResourcesPath(JNILoader.Arch.x86_64, JNILoader.OS.linux, JNILoader.Abi.gnu));

        assertEquals("/lib/aarch64-linux-gnu/libtb_jniclient.so", JNILoader
                .getResourcesPath(JNILoader.Arch.aarch64, JNILoader.OS.linux, JNILoader.Abi.gnu));

        assertEquals("/lib/x86_64-linux-musl/libtb_jniclient.so", JNILoader
                .getResourcesPath(JNILoader.Arch.x86_64, JNILoader.OS.linux, JNILoader.Abi.musl));

        assertEquals("/lib/aarch64-linux-musl/libtb_jniclient.so", JNILoader
                .getResourcesPath(JNILoader.Arch.aarch64, JNILoader.OS.linux, JNILoader.Abi.musl));

        assertEquals("/lib/x86_64-macos/libtb_jniclient.dylib", JNILoader
                .getResourcesPath(JNILoader.Arch.x86_64, JNILoader.OS.macos, JNILoader.Abi.none));

        assertEquals("/lib/aarch64-macos/libtb_jniclient.dylib", JNILoader
                .getResourcesPath(JNILoader.Arch.aarch64, JNILoader.OS.macos, JNILoader.Abi.none));

        assertEquals("/lib/x86_64-windows/tb_jniclient.dll", JNILoader
                .getResourcesPath(JNILoader.Arch.x86_64, JNILoader.OS.windows, JNILoader.Abi.none));
    }

    @Test(expected = AssertionError.class)
    public void testUnsupportedPlatform() {
        JNILoader.getResourcesPath(JNILoader.Arch.aarch64, JNILoader.OS.windows,
                JNILoader.Abi.none);
    }
}
