using System;
using System.Runtime.InteropServices;

namespace TigerBeetle
{
    [StructLayout(LayoutKind.Sequential, Size = SIZE)]
    public struct Account
    {
        #region InnerTypes

        [StructLayout(LayoutKind.Explicit, Size = SIZE)]
        private unsafe struct ReservedData
        {
            public const int SIZE = 48;

            [FieldOffset(0)]
            private fixed byte raw[SIZE];

            public ReadOnlySpan<T> AsReadOnlySpan<T>()
            {
                fixed (void* ptr = &this)
                {
                    return new ReadOnlySpan<T>(ptr, SIZE);
                }
            }

            public Span<T> AsSpan<T>()
            {
                fixed (void* ptr = &this)
                {
                    return new Span<T>(ptr, SIZE);
                }
            }
        }

        #endregion

        #region Fields

        public const int SIZE = 128;

        private UInt128 id;

        private UInt128 userData;

        private ReservedData reserved;

        private uint ledger;

        private ushort code;

        private AccountFlags flags;

        private ulong debitsPending;

        private ulong debitsPosted;

        private ulong creditsPending;

        private ulong creditsPosted;

        private ulong timestamp;

        #endregion Fields

        #region Properties

        public UInt128 Id { get => id; set => id = value; }

        public UInt128 UserData { get => userData; set => userData = value; }

        public ReadOnlySpan<byte> Reserved
        {
            get => reserved.AsReadOnlySpan<byte>();
			internal set => value.CopyTo(reserved.AsSpan<byte>());
        }

        public uint Ledger { get => ledger; set => ledger = value; }

        public ushort Code { get => code; set => code = value; }

        public AccountFlags Flags { get => flags; set => flags = value; }

        public ulong DebitsPending { get => debitsPending; internal set => debitsPending = value; }

        public ulong DebitsPosted { get => debitsPosted; internal set => debitsPosted = value; }

        public ulong CreditsPending { get => creditsPending; internal set => creditsPending = value; }

        public ulong CreditsPosted { get => creditsPosted; internal set => creditsPosted = value; }

        public ulong Timestamp { get => timestamp; internal set => timestamp = value; }

        #endregion Properties
    }
}
