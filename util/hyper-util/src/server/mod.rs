//! Server utilities.

pub mod conn;

#[cfg(feature = "server-graceful")]
pub mod graceful;

#[cfg(feature = "server-tcp")]
mod server;
#[cfg(feature = "server-tcp")]
mod tcp;

#[cfg(feature = "server-tcp")]
pub use server::Server;
#[cfg(feature = "server-tcp")]
pub use tcp::{AddrStream, Incoming};
