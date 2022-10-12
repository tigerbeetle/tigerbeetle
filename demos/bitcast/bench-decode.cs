using System;
using System.Runtime.InteropServices;
public static class BenchDecode
{
    const int TRANSFER = 128;

    [StructLayout(LayoutKind.Sequential, Size = 16)]
    unsafe struct UInt128
    {
        public fixed byte bytes[16];
    }    

    [StructLayout(LayoutKind.Sequential, Size = TRANSFER)]
    unsafe struct Transfer
    {
        public UInt128 id;
        public UInt128 debit_account_id;
        public UInt128 credit_account_id;
        public UInt128 user_data;
        public UInt128 reserved;
        public UInt128 pending_id;
        public ulong timeout;
        public uint ledger;
        public ushort code;
        public ushort flags;
        public ulong amount;
        public ulong timestamp;
    }

    public static void Main() {

        var buffer = Load("transfers");

        Console.WriteLine("do this a few times to let CLR optimize...");

        int loops = 10;
        while (loops-- > 0)
        {
            int offset = 0;
            ulong sum = 0UL;
            var now = DateTime.Now;

            while (offset < buffer.Length) 
            {
                var array = buffer[offset..][0..TRANSFER];
                
                // Deserialize without much overhead:
                var transfer = MemoryMarshal.AsRef<Transfer>(array);

                sum += transfer.amount;
                offset += TRANSFER;
            }

            TimeSpan elapsed = DateTime.Now - now;
            Console.WriteLine("  C#: sum of transfer amounts={0} ms={1:0.000}", sum, elapsed.TotalMilliseconds);
        }
    }

    static Span<byte> Load(string file)
    {
        return System.IO.File.ReadAllBytes(file).AsSpan(0);
    }



}