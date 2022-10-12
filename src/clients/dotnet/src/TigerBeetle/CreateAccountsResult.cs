using System.Runtime.InteropServices;

namespace TigerBeetle
{
    [StructLayout(LayoutKind.Sequential, Size = 8)]
    public struct CreateAccountsResult
    {
        #region Fields

        private readonly int index;

        private readonly CreateAccountResult result;

        #endregion Fields

        #region Constructor

        internal CreateAccountsResult(int index, CreateAccountResult result)
        {
            this.index = index;
            this.result = result;
        }

        #endregion Constructor

        #region Properties

        public int Index => index;

        public CreateAccountResult Result => result;

        #endregion Properties
    }
}
