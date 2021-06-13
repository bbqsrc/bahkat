#[cfg(all(target_os = "macos", feature = "macos"))]
pub mod macos;
#[cfg(feature = "prefix")]
pub mod prefix;
#[cfg(all(windows, feature = "windows"))]
pub mod windows;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use hashbrown::HashMap;
use pahkat_types::package::Package;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::config::Config;
use crate::repo::{PackageQuery, RepoDownloadError};
use crate::transaction::{install::InstallError, uninstall::UninstallError};
use crate::transaction::{PackageStatus, PackageStatusError, ResolvedPackageQuery, PackageDependencyStatusError};
use crate::types::repo::RepoUrl;
use crate::{LoadedRepository, PackageKey};

pub type SharedStoreConfig = Arc<RwLock<Config>>;
pub type SharedRepos = Arc<RwLock<HashMap<RepoUrl, LoadedRepository>>>;
pub type SharedRepoErrors = Arc<RwLock<HashMap<RepoUrl, RepoDownloadError>>>;

#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("Payload error")]
    Payload(#[from] crate::repo::PayloadError),

    #[error("IO error")]
    Io(#[from] std::io::Error),

    #[error("Invalid payload type")]
    InvalidPayloadType,
}

#[derive(Debug)]
pub enum ProgressEvent<P: Debug, C: Debug, E: Debug> {
    Progress(P),
    Complete(C),
    Error(E),
}

pub type DownloadEvent = ProgressEvent<(u64, u64), PathBuf, crate::download::DownloadError>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum InstallTarget {
    System,
    User,
}

impl InstallTarget {
    pub fn to_u8(&self) -> u8 {
        match self {
            InstallTarget::System => 0,
            InstallTarget::User => 1,
        }
    }
}

impl From<u8> for InstallTarget {
    fn from(value: u8) -> InstallTarget {
        match value {
            1 => InstallTarget::User,
            _ => InstallTarget::System,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub struct LocalizedStrings {
    pub tags: HashMap<String, String>,
    pub channels: HashMap<String, String>,
}

impl Default for InstallTarget {
    fn default() -> Self {
        InstallTarget::System
    }
}

pub type Stream<T> = Pin<Box<dyn futures::stream::Stream<Item = T> + Send + 'static>>;
pub type Future<T> = Pin<Box<dyn std::future::Future<Output = T> + Send + Sync + 'static>>;

pub trait PackageStore: Send + Sync {
    fn repos(&self) -> SharedRepos;
    fn errors(&self) -> SharedRepoErrors;
    fn config(&self) -> SharedStoreConfig;

    #[must_use]
    fn download(&self, key: &PackageKey) -> Stream<DownloadEvent>;

    fn import(&self, key: &PackageKey, installer_path: &Path) -> Result<PathBuf, ImportError>;

    fn install(
        &self,
        key: &PackageKey,
        target: InstallTarget,
    ) -> Result<PackageStatus, InstallError>;

    fn uninstall(
        &self,
        key: &PackageKey,
        target: InstallTarget,
    ) -> Result<PackageStatus, UninstallError>;

    fn status(
        &self,
        key: &PackageKey,
        target: InstallTarget,
    ) -> Result<PackageStatus, PackageStatusError>;

    fn dependency_status(
        &self,
        key: &PackageKey,
        target: InstallTarget,
    ) -> Result<Vec<(PackageKey, PackageStatus)>, PackageDependencyStatusError>;

    fn all_statuses(
        &self,
        repo_url: &RepoUrl,
        target: InstallTarget,
    ) -> BTreeMap<String, Result<PackageStatus, PackageStatusError>>;

    fn find_package_by_id(&self, package_id: &str) -> Option<(PackageKey, Package)>;

    fn find_package_by_key(&self, key: &PackageKey) -> Option<Package>;

    #[must_use]
    fn refresh_repos(&self) -> Future<Result<(), HashMap<RepoUrl, RepoDownloadError>>>;

    #[must_use]
    fn force_refresh_repos(&self) -> Future<Result<(), HashMap<RepoUrl, RepoDownloadError>>> {
        self.clear_cache();
        self.refresh_repos()
    }

    fn clear_cache(&self);

    fn strings(&self, language: String) -> Future<HashMap<RepoUrl, LocalizedStrings>>;

    // #[export::experimental]
    fn resolve_package_query(
        &self,
        query: PackageQuery,
        install_target: &[InstallTarget],
    ) -> ResolvedPackageQuery;
}
