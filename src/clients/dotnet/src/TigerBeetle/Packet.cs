using static TigerBeetle.TBClient;

namespace TigerBeetle
{
    internal struct Packet
    {
        #region Fields

        public readonly unsafe TBPacket* Data;

        #endregion Fields

        #region Constructor

        public unsafe Packet(TBPacket* data)
        {
            Data = data;
        }

        #endregion Constructor
    }
}
