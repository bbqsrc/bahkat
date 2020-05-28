use std::path::Path;
use std::sync::Arc;
use futures::stream::StreamExt;

use pahkat_client::{
    transaction::{PackageAction, PackageTransaction},
    package_store::InstallTarget,
    PackageStore,
    PackageKey, DownloadEvent,
};

use pahkat_client::config::RepoRecord;
use pahkat_types::repo::RepoUrl;
use crate::Platform;

pub(crate) async fn config<'a>(
    store: Arc<dyn PackageStore>,
    config: &'a crate::cli::command::Config,
    target: InstallTarget,
    args: &'a crate::Args,
) -> Result<(), anyhow::Error> {
    match config {
        crate::cli::command::Config::Repo(repo) => match repo {
            crate::cli::command::config::Repo::Add(a) => {
                let url = a.repo_url.to_owned();
                let channel = a.channel.to_owned();

                let config = store.config();
                let mut config = config.write().unwrap();

                let repos = config.repos_mut();
                repos.insert(url, RepoRecord {
                    channel
                });

                Ok(())

            }
            crate::cli::command::config::Repo::Remove(a) => {
                let url = &a.repo_url;
                Ok(())
            }
            crate::cli::command::config::Repo::List(a) => {
                Ok(())
            }
        }   
    }
}
