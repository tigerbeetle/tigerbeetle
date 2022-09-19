package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class JNILoaderTest {

    @Test
    public void testResourcePath() {

        assertEquals("/lib/linux-x86_64/libtb_jniclient.so",
                JNILoader.getResourcesPath(JNILoader.OS.linux, JNILoader.Arch.x86_64));

        assertEquals("/lib/linux-aarch64/libtb_jniclient.so",
                JNILoader.getResourcesPath(JNILoader.OS.linux, JNILoader.Arch.aarch64));

        assertEquals("/lib/macos-x86_64/libtb_jniclient.dylib",
                JNILoader.getResourcesPath(JNILoader.OS.macos, JNILoader.Arch.x86_64));

        assertEquals("/lib/macos-aarch64/libtb_jniclient.dylib",
                JNILoader.getResourcesPath(JNILoader.OS.macos, JNILoader.Arch.aarch64));

        assertEquals("/lib/win-x86_64/tb_jniclient.dll",
                JNILoader.getResourcesPath(JNILoader.OS.win, JNILoader.Arch.x86_64));
    }

    @Test(expected = AssertionError.class)
    public void testUnsuportedPlatform() {
        JNILoader.getResourcesPath(JNILoader.OS.win, JNILoader.Arch.aarch64);
    }

}
