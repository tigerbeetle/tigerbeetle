package com.tigerbeetle;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.stream.Collectors;

class Server implements AutoCloseable {

    public static final String TB_SERVER = "../../../zig-out/bin/tigerbeetle";

    public final String tb_file;
    private final Process process;
    private String address;

    public Server(final String label) throws IOException, Exception, InterruptedException {
        this.tb_file = "./0_0.tigerbeetle." + label;

        cleanUp();

        String exe;
        switch (JNILoader.OS.getOS()) {
            case windows:
                exe = TB_SERVER + ".exe";
                break;
            default:
                exe = TB_SERVER;
                break;
        }

        final var format = Runtime.getRuntime().exec(new String[] {exe, "format", "--cluster=0",
                "--replica=0", "--replica-count=1", "--development", tb_file});
        if (format.waitFor() != 0) {
            final var reader = new BufferedReader(new InputStreamReader(format.getErrorStream()));
            final var error = reader.lines().collect(Collectors.joining(". "));
            throw new Exception("Format failed. " + error);
        }

        this.process = new ProcessBuilder()
                .command(new String[] {exe, "start", "--addresses=0", "--development", tb_file})
                .redirectOutput(Redirect.PIPE).redirectError(Redirect.INHERIT).start();

        final var stdout = process.getInputStream();
        try (final var reader = new BufferedReader(new InputStreamReader(stdout))) {
            this.address = reader.readLine().trim();
        }
    }

    public String getAddress() {
        return address;
    }

    @Override
    public void close() throws Exception {
        cleanUp();
    }

    private void cleanUp() throws Exception {
        try {
            if (process != null && process.isAlive()) {
                process.destroy();
            }

            final var file = new File("./" + tb_file);
            file.delete();
        } catch (Throwable any) {
            throw new Exception("Cleanup has failed");
        }
    }
}
