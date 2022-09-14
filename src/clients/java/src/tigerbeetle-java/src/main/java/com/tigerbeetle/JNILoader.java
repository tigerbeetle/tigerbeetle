package com.tigerbeetle;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

final class JNILoader {

    private enum OS {
        windows,
        linux,
        macos,
    }

    private enum Arch {
        x86_64,
        aarch64,
    }

    private JNILoader() {}

    public static final String libName = "tb_jniclient";

    public static void loadFromJar() throws IOException {

        final String jniResourcesPath = getResourcesPath();
        final String fileName = Paths.get(jniResourcesPath).getFileName().toString();

        File temp;

        try (InputStream stream = JNILoader.class.getResourceAsStream(jniResourcesPath)) {

            if (stream == null)
                throw new AssertionError("Tigerbeetle jni %s could not be found.", fileName);

            temp = Files.createTempFile(fileName, "").toFile();
            Files.copy(stream, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException ioException) {
            throw new AssertionError(ioException,
                    "Tigerbeetle jni %s could not be extracted from jar.", fileName);
        }

        System.load(temp.getAbsolutePath());
        temp.deleteOnExit();
    }

    private static String getResourcesPath() {

        OS os = getOS();
        Arch arch = getArch();

        final String jniResources = String.format("/lib/%s-%s", os, arch);

        switch (os) {
            case linux:

                return String.format("%s/lib%s.so", jniResources, libName);

            case macos:

                return String.format("%s/lib%s.dylib", jniResources, libName);

            case windows:

                if (arch == Arch.x86_64)
                    return String.format("%s/%s.dll", jniResources, libName);
                break;

        }

        throw new AssertionError("Unsupported OS-arch %s-%s", os, arch);
    }

    private static OS getOS() {
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.startsWith("win")) {
            return OS.windows;
        } else if (osName.startsWith("macos") || osName.startsWith("osx")
                || osName.startsWith("darwin")) {
            return OS.macos;
        } else if (osName.startsWith("linux")) {
            return OS.linux;
        } else {
            throw new AssertionError(String.format("Unsuported OS %s", osName));
        }
    }

    private static Arch getArch() {
        String osArch = System.getProperty("os.arch").toLowerCase();

        if (osArch.startsWith("x86_64") || osArch.startsWith("amd64") || osArch.startsWith("x64")) {
            return Arch.x86_64;
        } else if (osArch.startsWith("aarch64")) {
            return Arch.aarch64;
        } else {
            throw new AssertionError(String.format("Unsuported OS arch %s", osArch));
        }
    }
}
