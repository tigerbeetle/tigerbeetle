﻿using System;
using System.Runtime.InteropServices;

namespace TigerBeetle
{
    [StructLayout(LayoutKind.Sequential, Size = SIZE)]
    public struct Transfer
    {
        #region InnerTypes

        [StructLayout(LayoutKind.Explicit, Size = SIZE)]
        private unsafe struct ReservedData
        {
            public const int SIZE = 16;

            [FieldOffset(0)]
            private fixed byte raw[16];

            public ReadOnlySpan<T> AsReadOnlySpan<T>()
            {
                fixed (void* ptr = raw)
                {
                    return new ReadOnlySpan<T>(ptr, SIZE);
                }
            }

            public Span<T> AsSpan<T>()
            {
                fixed (void* ptr = raw)
                {
                    return new Span<T>(ptr, SIZE);
                }
            }
        }

        #endregion

        #region Fields

        public const int SIZE = 128;

        private UInt128 id;

        private UInt128 debitAccountId;

        private UInt128 creditAccountId;

        private UInt128 userData;

        private ReservedData reserved;

        private UInt128 pendingId;

        private ulong timeout;

        private uint ledger;

        private ushort code;

        private TransferFlags flags;

        private ulong amount;

        private ulong timestamp;

        #endregion Fields

        #region Properties

        public UInt128 Id { get => id; set => id = value; }

        public UInt128 DebitAccountId { get => debitAccountId; set => debitAccountId = value; }

        public UInt128 CreditAccountId { get => creditAccountId; set => creditAccountId = value; }

        public UInt128 UserData { get => userData; set => userData = value; }

        public ReadOnlySpan<byte> Reserved
        {
            get => reserved.AsReadOnlySpan<byte>();
            internal set => value.CopyTo(reserved.AsSpan<byte>());
        }

        public UInt128 PendingId { get => pendingId; set => pendingId = value; }

        public ulong Timeout { get => timeout; set => timeout = value; }

        public uint Ledger { get => ledger; set => ledger = value; }

        public ushort Code { get => code; set => code = value; }

        public TransferFlags Flags { get => flags; set => flags = value; }

        public ulong Amount { get => amount; set => amount = value; }

        public ulong Timestamp { get => timestamp; internal set => timestamp = value; }

        #endregion Properties
    }
}
