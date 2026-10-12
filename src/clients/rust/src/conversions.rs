pub use super::*;

impl From<i32> for InitError {
    fn from(other: i32) -> InitError {
        use tbc::*;
        use InitError::*;

        match other {
            TB_INIT_STATUS_TB_INIT_SUCCESS => panic!(),
            TB_INIT_STATUS_TB_INIT_UNEXPECTED => Unexpected,
            TB_INIT_STATUS_TB_INIT_OUT_OF_MEMORY => OutOfMemory,
            TB_INIT_STATUS_TB_INIT_ADDRESS_INVALID => AddressInvalid,
            TB_INIT_STATUS_TB_INIT_ADDRESS_LIMIT_EXCEEDED => AddressLimitExceeded,
            TB_INIT_STATUS_TB_INIT_SYSTEM_RESOURCES => SystemResources,
            TB_INIT_STATUS_TB_INIT_NETWORK_SUBSYSTEM => NetworkSubsystem,
            v => panic!("Unknown InitError: {v}"),
        }
    }
}

impl From<InitError> for i32 {
    fn from(other: InitError) -> i32 {
        use tbc::*;
        use InitError::*;

        match other {
            Unexpected => TB_INIT_STATUS_TB_INIT_UNEXPECTED,
            OutOfMemory => TB_INIT_STATUS_TB_INIT_OUT_OF_MEMORY,
            AddressInvalid => TB_INIT_STATUS_TB_INIT_ADDRESS_INVALID,
            AddressLimitExceeded => TB_INIT_STATUS_TB_INIT_ADDRESS_LIMIT_EXCEEDED,
            SystemResources => TB_INIT_STATUS_TB_INIT_SYSTEM_RESOURCES,
            NetworkSubsystem => TB_INIT_STATUS_TB_INIT_NETWORK_SUBSYSTEM,
        }
    }
}

impl From<u8> for Error {
    fn from(other: u8) -> Error {
        use tbc::*;
        use Error::*;

        match other {
            TB_PACKET_STATUS_TB_PACKET_OK => panic!(),
            TB_PACKET_STATUS_TB_PACKET_TOO_MUCH_DATA => TooMuchData,
            TB_PACKET_STATUS_TB_PACKET_CLIENT_EVICTED => ClientEvicted,
            TB_PACKET_STATUS_TB_PACKET_CLIENT_RELEASE_TOO_LOW => ClientReleaseTooLow,
            TB_PACKET_STATUS_TB_PACKET_CLIENT_RELEASE_TOO_HIGH => ClientReleaseTooHigh,
            TB_PACKET_STATUS_TB_PACKET_CLIENT_CLOSED => ClientClosed,
            TB_PACKET_STATUS_TB_PACKET_INVALID_OPERATION => InvalidOperation,
            TB_PACKET_STATUS_TB_PACKET_INVALID_DATA_SIZE => InvalidDataSize,
            v => panic!("Unknown Error: {v}"),
        }
    }
}

impl From<Error> for u8 {
    fn from(other: Error) -> u8 {
        use tbc::*;
        use Error::*;

        match other {
            TooMuchData => TB_PACKET_STATUS_TB_PACKET_TOO_MUCH_DATA,
            ClientEvicted => TB_PACKET_STATUS_TB_PACKET_CLIENT_EVICTED,
            ClientReleaseTooLow => TB_PACKET_STATUS_TB_PACKET_CLIENT_RELEASE_TOO_LOW,
            ClientReleaseTooHigh => TB_PACKET_STATUS_TB_PACKET_CLIENT_RELEASE_TOO_HIGH,
            ClientClosed => TB_PACKET_STATUS_TB_PACKET_CLIENT_CLOSED,
            InvalidOperation => TB_PACKET_STATUS_TB_PACKET_INVALID_OPERATION,
            InvalidDataSize => TB_PACKET_STATUS_TB_PACKET_INVALID_DATA_SIZE,
        }
    }
}
