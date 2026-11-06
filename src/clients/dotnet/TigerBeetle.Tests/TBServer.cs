using System;
using System.Diagnostics;
using System.IO;

namespace TigerBeetle.Tests;

internal class TBServer : IDisposable
{
    private readonly Process process;
    private readonly string dataFile;

    public string Address { get; }

    public TBServer()
    {
        dataFile = Path.GetRandomFileName();
        var tigerbeetleBinary = Environment.GetEnvironmentVariable("TIGERBEETLE_BINARY");
        if (tigerbeetleBinary == null)
        {
            throw new InvalidOperationException("TIGERBEETLE_BINARY environmental variable is required");
        }

        {
            using var format = new Process();
            format.StartInfo.FileName = tigerbeetleBinary;
            format.StartInfo.Arguments = $"format --cluster=0 --replica=0 --replica-count=1 --development ./{dataFile}";
            format.StartInfo.RedirectStandardError = true;
            format.Start();
            var formatStderr = format.StandardError.ReadToEnd();
            format.WaitForExit();
            if (format.ExitCode != 0) throw new InvalidOperationException($"format failed, ExitCode={format.ExitCode} stderr:\n{formatStderr}");
        }

        process = new Process();
        process.StartInfo.FileName = tigerbeetleBinary;
        process.StartInfo.Arguments = $"start --addresses=0 --development ./{dataFile}";
        process.StartInfo.RedirectStandardInput = true;
        process.StartInfo.RedirectStandardOutput = true;
        process.Start();

        Address = process.StandardOutput.ReadLine()!.Trim();
    }

    public void Dispose()
    {
        process.Kill();
        process.WaitForExit();
        process.Dispose();
        File.Delete($"./{dataFile}");
    }
}
