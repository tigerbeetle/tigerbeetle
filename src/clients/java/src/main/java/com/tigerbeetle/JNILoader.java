package com.tigerbeetle;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

final class JNILoader {

    enum OS {
        windows,
        linux,
        macos;

        public static OS getOS() {
            String osName = System.getProperty("os.name").toLowerCase();
            if (osName.startsWith("win")) {
                return OS.windows;
            } else if (osName.startsWith("mac") || osName.startsWith("darwin")) {
                return OS.macos;
            } else if (osName.startsWith("linux")) {
                return OS.linux;
            } else {
                throw new AssertionError(String.format("Unsupported OS %s", osName));
            }
        }
    }

    enum Arch {
        x86_64,
        aarch64;

        public static Arch getArch() {
            String osArch = System.getProperty("os.arch").toLowerCase();

            if (osArch.startsWith("x86_64") || osArch.startsWith("amd64")
                    || osArch.startsWith("x64")) {
                return Arch.x86_64;
            } else if (osArch.startsWith("aarch64")) {
                return Arch.aarch64;
            } else {
                throw new AssertionError(String.format("Unsupported OS arch %s", osArch));
            }
        }
    }

    enum Abi {
        none,
        gnu,
        musl;

        public static Abi getAbi(OS os) {
            if (os != OS.linux)
                return Abi.none;

            /**
             * We need to detect during runtime which libc the JVM uses to load the correct JNI lib.
             *
             * Rationale: The /proc/self/map_files/ subdirectory contains entries corresponding to
             * memory-mapped files loaded by the JVM.
             * https://man7.org/linux/man-pages/man5/proc.5.html: We detect a musl-based distro by
             * checking if any library contains the name "musl".
             *
             * Prior art: https://github.com/xerial/sqlite-jdbc/issues/623
             */

            final var mapFiles = Paths.get("/proc/self/map_files");
            try (var stream = Files.newDirectoryStream(mapFiles)) {
                for (final Path path : stream) {
                    try {
                        final var libName = path.toRealPath().toString().toLowerCase();
                        if (libName.contains("musl")) {
                            return Abi.musl;
                        }
                    } catch (IOException exception) {
                        continue;
                    }
                }
            } catch (IOException exception) {
            }

            return Abi.gnu;
        }
    }

    private JNILoader() {}

    public static final String libName = "tb_jniclient";

    public static void loadFromJar() {

        Arch arch = Arch.getArch();
        OS os = OS.getOS();
        Abi abi = Abi.getAbi(os);

        final String jniResourcesPath = getResourcesPath(arch, os, abi);
        final String fileName = Paths.get(jniResourcesPath).getFileName().toString();

        File temp;

        try (InputStream stream = JNILoader.class.getResourceAsStream(jniResourcesPath)) {

            if (stream == null) {
                // It's not expected when running from the jar package.
                // If not found, we fallback to the standard JVM path and let the
                // UnsatisfiedLinkError alert if it couldn't be found there.
                System.loadLibrary(libName);
                return;
            }

            temp = Files.createTempFile(fileName, "").toFile();
            Files.copy(stream, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException ioException) {
            throw new AssertionError(ioException,
                    "TigerBeetle jni %s could not be extracted from jar.", fileName);
        }

        System.load(temp.getAbsolutePath());
        temp.deleteOnExit();
    }

    static String getResourcesPath(Arch arch, OS os, Abi abi) {

        final String jniResources = String.format("/lib/%s-%s", arch, os);

        switch (os) {
            case linux:

                return String.format("%s-%s/lib%s.so", jniResources, abi, libName);

            case macos:

                return String.format("%s/lib%s.dylib", jniResources, libName);

            case windows:

                if (arch == Arch.x86_64)
                    return String.format("%s/%s.dll", jniResources, libName);
                break;

        }

        throw new AssertionError("Unsupported OS-arch %s-%s", os, arch);
    }
}
