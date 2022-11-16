using System.Runtime.InteropServices;

namespace TigerBeetle
{
    [StructLayout(LayoutKind.Sequential, Size = SIZE)]
    public struct CreateTransfersResult
    {
        #region Fields

        public const int SIZE = 8;

        private readonly int index;

        private readonly CreateTransferResult result;

        #endregion Fields

        #region Constructor

        internal CreateTransfersResult(int index, CreateTransferResult result)
        {
            this.index = index;
            this.result = result;
        }

        #endregion Constructor

        #region Properties

        public int Index => index;

        public CreateTransferResult Result => result;

        #endregion Properties
    }
}
