use amq_proto;
use futures::sync::mpsc;
#[cfg(feature = "tls")]
use openssl;
use std::convert::From;
use std::{io, error, fmt};
use url;

#[derive(Debug, Clone)]
pub enum AMQPError {
    DecodeError(&'static str),
    FramingError(String),
    IoError(io::ErrorKind),
    MpscReceiveError,
    MpscSendError,
    Protocol(String),
    QueueEmpty,
    SchemeError(String),
    SyncError,
    UrlParseError(url::ParseError),
    VHostError,

    /// We may add other errors to this type. In order to preserve API
    /// compatibility, we add a hidden, unused member, so that callers
    /// won't (in theory) exhaustively match on all possible errors.
    #[doc(hide)]
    __Nonexhaustive,
}

impl fmt::Display for AMQPError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AMQP ")?;
        match *self {
            AMQPError::DecodeError(err) => {
                write!(f, "decoding error: {}", err)
            }
            AMQPError::FramingError(ref err) => {
                write!(f, "framing error: {}", err)
            }
            AMQPError::IoError(err) => {
                write!(f, "I/O error: {:?}", err)
            }
            AMQPError::MpscReceiveError => {
                write!(f, "tried to receive from closed channel")
            }
            AMQPError::MpscSendError => {
                write!(f, "tried to write to closed channel")
            }
            AMQPError::Protocol(ref err) => {
                write!(f, "protocol error: {}", err)
            }
            AMQPError::QueueEmpty => {
                write!(f, "queue is empty")
            }
            AMQPError::SchemeError(ref err) => {
                write!(f, "URL scheme error: {}", err)
            }
            AMQPError::SyncError => {
                write!(f, "thread panicked while holding a lock")
            }
            AMQPError::UrlParseError(ref err) => {
                write!(f, "URL parse error: {}", err)
            }
            AMQPError::VHostError => {
                write!(f, "access to vhost is denied for a current user")
            }
            AMQPError::__Nonexhaustive => {
                write!(f, "placeholder error that should never appear")
            }
        }
    }
}

impl error::Error for AMQPError {
    fn description<'a>(&'a self) -> &'a str {
        match *self {
            AMQPError::DecodeError(err) => err,
            AMQPError::FramingError(ref err) => err,
            AMQPError::IoError(_) => "I/O error",
            AMQPError::MpscReceiveError => "tried to read from closed channel",
            AMQPError::MpscSendError => "tried to write to closed channel",
            AMQPError::Protocol(ref err) => err,
            AMQPError::QueueEmpty => "queue is empty",
            AMQPError::SchemeError(ref err) => err,
            AMQPError::SyncError => "synchronization error",
            AMQPError::UrlParseError(_) => "URL parsing error",
            AMQPError::VHostError => "access to vhost is denied for a current user",
            AMQPError::__Nonexhaustive =>"placeholder error that should never appear",
        }
    }
}

pub type AMQPResult<T> = Result<T, AMQPError>;

impl From<io::Error> for AMQPError {
    fn from(err: io::Error) -> AMQPError {
        AMQPError::IoError(err.kind())
    }
}

impl <T> From<::std::sync::PoisonError<T>> for AMQPError {
    fn from(_ : ::std::sync::PoisonError<T>) -> AMQPError {
        AMQPError::SyncError
    }
}

impl From<url::ParseError> for AMQPError {
    fn from(err: url::ParseError) -> AMQPError {
        AMQPError::UrlParseError(err)
    }
}

impl From<amq_proto::Error> for AMQPError {
    fn from(err: amq_proto::Error) -> AMQPError {
        AMQPError::Protocol(format!("{}", err))
    }
}

#[cfg(feature = "tls")]
impl From<openssl::ssl::Error> for AMQPError {
    fn from(err: openssl::ssl::Error) -> AMQPError {
        AMQPError::Protocol(format!("{}", err))
    }
}

impl<T> From<mpsc::SendError<T>> for AMQPError {
    fn from(_err: mpsc::SendError<T>) -> AMQPError {
        AMQPError::MpscSendError
    }
}
