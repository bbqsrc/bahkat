#![recursion_limit = "2048"]

pub extern crate pahkat_types as types;

#[cfg(feature = "ffi")]
pub mod ffi;

pub mod defaults;
pub mod package_store;
pub mod repo;
pub mod transaction;

mod cmp;
mod config;
mod download;
mod ext;
mod fbs;

pub use self::config::{Config, Permission};
pub use self::download::Download;
pub use self::package_store::{PackageStore, DownloadEvent};
pub use self::repo::{LoadedRepository, PackageKey};
pub use self::transaction::{PackageAction, PackageActionType, PackageTransaction};

#[cfg(all(target_os = "macos", feature = "macos"))]
pub use package_store::macos::MacOSPackageStore;

#[cfg(feature = "prefix")]
pub use package_store::prefix::PrefixPackageStore;

#[cfg(all(windows, feature = "windows"))]
pub use package_store::windows::WindowsPackageStore;

pub(crate) use fbs::generated::pahkat as pahkat_fbs;
