use std::path::Path;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::pahkat_fbs;

#[derive(Debug, thiserror::Error)]
pub enum RepoDownloadError {
    #[error("Error while processing HTTP request")]
    ReqwestError(#[from] reqwest::Error),

    #[error("Error parsing TOML index")]
    TomlError(#[from] toml::de::Error),

    #[error("I/O error")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoadedRepositoryMeta {
    pub channel: Option<String>,
    // pub hash_id: String,
    // TODO: last update
}

#[derive(Debug)]
pub struct LoadedRepository {
    info: pahkat_types::repo::Index,
    packages: Box<[u8]>,
    // strings: pahkat_types::strings::
    meta: LoadedRepositoryMeta,
}

impl LoadedRepository {
    pub async fn from_cache_or_url(
        url: &Url,
        channel: Option<String>,
        cache_dir: &Path,
    ) -> Result<LoadedRepository, RepoDownloadError> {
        Self::from_url(url, channel).await
    }

    async fn from_url(url: &Url, channel: Option<String>) -> Result<LoadedRepository, RepoDownloadError> {
        let client = reqwest::Client::new();

        log::trace!("Loading repo: {} channel:{:?}", &url, &channel);

        let info = client.get(&format!("{}/index.toml", url)).send().await?.text().await?;
        let info: pahkat_types::repo::Index = toml::from_str(&info)?;

        let packages = client
            .get(&format!("{}/packages/index.bin", url))
            .send()
            .await?
            .bytes()
            .await?
            .to_vec()
            .into_boxed_slice();

        let repo = LoadedRepository {
            info,
            packages,
            meta: LoadedRepositoryMeta {
                channel,
                // hash_id: "".into(),
            },
        };

        log::trace!("Loaded.");
        Ok(repo)
    }

    pub fn info(&self) -> &pahkat_types::repo::Index {
        &self.info
    }

    pub fn packages<'a>(&'a self) -> pahkat_fbs::Packages<&'a [u8]> {
        pahkat_fbs::Packages::get_root(&*self.packages).expect("packages must always exist")
    }

    pub fn meta(&self) -> &LoadedRepositoryMeta {
        &self.meta
    }
}
