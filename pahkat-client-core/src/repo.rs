mod repository;

use futures::Future;
pub use pahkat_types::PackageKey;
pub use repository::{LoadedRepository, RepoDownloadError};

use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::hash::Hash;

use crossbeam_queue::SegQueue;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use hashbrown::{HashMap, HashSet};
use sha2::digest::Digest;
use sha2::Sha256;
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle};

use crate::config::Config;
use crate::defaults;
use crate::fbs::PackagesExt;
use crate::package_store::DownloadEvent;
use crate::package_store::PackageStore;
use crate::transaction::{
    PackageStatus, PackageStatusError, ResolvedDescriptor, ResolvedPackageQuery,
};
use pahkat_types::package::{Descriptor, Package, Release, Version};
use pahkat_types::payload::Target;
use pahkat_types::repo::RepoUrl;

#[derive(Debug, Clone, Error)]
pub enum PayloadError {
    #[error("No package found")]
    NoPackage,
    #[error("Invalid package found")]
    NoConcretePackage,
    #[error("No payload found meeting query criteria")]
    NoPayloadFound,
    #[error("Some criteria is not met for the current payload")]
    CriteriaUnmet(String),
}

#[derive(Debug, Clone)]
pub struct ReleaseQuery<'a> {
    pub platform: &'a str,
    pub arch: Option<&'a str>,
    pub channels: Vec<&'a str>,
    pub versions: Vec<VersionQuery<'a>>,
    pub payloads: Vec<&'a str>,
}

impl<'a> ReleaseQuery<'a> {
    pub(crate) fn and_payloads(mut self, payloads: Vec<&'a str>) -> Self {
        self.payloads = payloads;
        self
    }
}

impl<'a> Default for ReleaseQuery<'a> {
    fn default() -> Self {
        Self {
            platform: defaults::platform(),
            arch: defaults::arch(),
            channels: vec![],
            versions: vec![],
            payloads: defaults::payloads().to_vec(),
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum VersionQuery<'a> {
    Match(&'a str),
    Semantic(semver::VersionReq),
}

impl<'a> VersionQuery<'a> {
    fn any_semantic() -> Self {
        VersionQuery::Semantic(semver::VersionReq::parse("*").unwrap())
    }

    fn matches(&self, version: &Version) -> bool {
        match (self, version) {
            (VersionQuery::Semantic(mask), Version::Semantic(v)) => mask.matches(v),
            _ => false,
        }
    }
}

#[inline(always)]
fn empty_payloads() -> &'static [&'static str] {
    &[]
}

pub(crate) struct ReleaseQueryIter<'a> {
    query: &'a ReleaseQuery<'a>,
    descriptor: &'a pahkat_types::package::Descriptor,
    next_release: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ReleaseQueryResponse<'a> {
    pub release: &'a Release,
    pub target: &'a Target,
}

impl<'a> ReleaseQueryIter<'a> {
    #[inline(always)]
    fn next_release(&mut self) -> Option<ReleaseQueryResponse<'a>> {
        log::trace!("Beginning release query iter: {:?}", &self.query);

        while let Some(release) = self.descriptor.release.get(self.next_release) {
            log::trace!(
                "Candidate release: version:{:?}, channel:{:?}",
                &release.version.to_string(),
                &release.channel
            );

            // If query is empty, it means search only for the main empty channel
            if let Some(channel) = release.channel.as_ref().map(|x| x.as_str()) {
                if !self.query.channels.contains(&channel) {
                    log::trace!("Skipping (not accepted channel)");
                    self.next_release += 1;
                    continue;
                }
            } else if release.channel.is_some() && !self.query.channels.is_empty() {
                log::trace!("Skipping (query channels not empty and no match)");
                self.next_release += 1;
                continue;
            }

            if let Some(payload) = self.next_payload(release) {
                log::trace!("Target resolved: {:?}", &payload.target);
                self.next_release += 1;
                return Some(payload);
            }

            self.next_release += 1;
            continue;
        }

        None
    }

    #[inline(always)]
    fn next_payload(&mut self, release: &'a Release) -> Option<ReleaseQueryResponse<'a>> {
        for ref target in release.target.iter() {
            log::trace!(
                "Candidate target: platform:{} arch:{:?}",
                &target.platform,
                &target.arch
            );

            if target.platform != self.query.platform {
                log::trace!("Skipping (platform does not match)");
                continue;
            }

            if let Some(arch) = self.query.arch {
                if let Some(ref target_arch) = target.arch {
                    if target_arch != arch {
                        log::trace!("Skipping (arch does not match)");
                        continue;
                    }
                }
            } else if target.arch.is_some() {
                log::trace!("Skipping (no arch in query but arch in target)");
                continue;
            }

            return Some(ReleaseQueryResponse { release, target });
        }

        None
    }
}

impl<'a> Iterator for ReleaseQueryIter<'a> {
    type Item = ReleaseQueryResponse<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_release()
    }
}

impl<'a> ReleaseQuery<'a> {
    pub(crate) fn iter(
        &'a self,
        descriptor: &'a pahkat_types::package::Descriptor,
    ) -> ReleaseQueryIter<'a> {
        ReleaseQueryIter {
            query: self,
            descriptor,
            next_release: 0,
        }
    }

    pub fn new(key: &'a PackageKey, repos: &'a HashMap<RepoUrl, LoadedRepository>) -> Self {
        let channels = key
            .query
            .channel
            .as_ref()
            .map(|x| vec![&**x])
            .unwrap_or_else(|| {
                repos
                    .iter()
                    .find_map(|(url, repo)| {
                        if &key.repository_url == url {
                            let channel = repo.meta().channel.as_ref().map(|x| &**x);
                            channel
                        } else {
                            None
                        }
                    })
                    .map(|x| vec![x])
                    .unwrap_or_else(|| vec![])
            });

        ReleaseQuery {
            platform: key
                .query
                .platform
                .as_ref()
                .map(|x| &**x)
                .unwrap_or_else(|| defaults::platform()),
            arch: key
                .query
                .arch
                .as_ref()
                .map(|x| &**x)
                .or_else(|| defaults::arch()),
            channels,
            versions: key
                .query
                .version
                .as_ref()
                .map(|v| vec![VersionQuery::Match(&*v)])
                .unwrap_or_else(|| vec![]),
            payloads: defaults::payloads().to_vec(),
        }
    }
}

pub(crate) fn resolve_package<'a>(
    package_key: &PackageKey,
    repos: &'a HashMap<RepoUrl, LoadedRepository>,
) -> Result<pahkat_types::package::Descriptor, PayloadError> {
    log::trace!("Finding package: {}", &package_key);
    let package = find_package_by_key(package_key, repos).ok_or(PayloadError::NoPackage)?;
    log::trace!("Package found: {}", &package_key);
    let descriptor: pahkat_types::package::Descriptor = package
        .try_into()
        .map_err(|_| PayloadError::NoConcretePackage)?;
    Ok(descriptor)
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct PackageQuery {
    pub keys: Option<Vec<PackageKey>>,
    pub tags: Option<Vec<String>>,
    pub channel: Option<String>,
}

pub(crate) fn resolve_package_query<'a>(
    store: &dyn PackageStore,
    query: &PackageQuery,
    install_target: &[InstallTarget],
    repos: &'a HashMap<RepoUrl, LoadedRepository>,
) -> ResolvedPackageQuery {
    log::debug!("resolve_package_query {:?} {:?}", query, install_target);

    use crate::fbs::DescriptorExt;

    // Only supports tags right now
    if let Some(tags) = query.tags.as_ref() {
        log::debug!("In tags");
        let descriptors: Vec<ResolvedDescriptor> = repos
            .values()
            .flat_map(|repo| {
                let repo_url = repo.info().repository.url.clone();

                log::debug!("Repo: {:?}", repo_url);

                // Collect all matching descriptors into one list
                repo.packages()
                    .packages()
                    .unwrap()
                    .iter()
                    .map(|(_, pkg)| pkg)
                    .filter(|pkg| {
                        let pkg_tags = pkg.tags().unwrap().unwrap();
                        pkg_tags.iter().any(|x| {
                            let t = x.unwrap().to_string();
                            log::debug!("Tag: {:?}", t);
                            tags.contains(&t)
                        })
                    })
                    .filter_map(move |pkg| {
                        let key = PackageKey::new_unchecked(
                            repo_url.clone(),
                            pkg.id().unwrap().to_string(),
                            None,
                        );
                        let status = install_target.iter().fold(None, |acc, cur| match acc {
                            Some(v) if v != PackageStatus::NotInstalled => Some(v),
                            _ => store.status(&key, *cur).ok(),
                        })?;

                        let descriptor = Descriptor::try_from(&pkg).ok()?;

                        ReleaseQuery::new(&key, repos)
                            .iter(&descriptor)
                            .next()
                            .map(|x| ResolvedDescriptor {
                                key: key.clone(),
                                status,
                                tags: descriptor.package.tags.clone(),
                                name: descriptor.name.clone(),
                                description: descriptor.description.clone(),
                                release: crate::transaction::ResolvedRelease::new(
                                    x.release.clone(),
                                    x.target.clone(),
                                ),
                            })
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let status = descriptors
            .iter()
            .fold(PackageStatus::UpToDate, |acc, cur| {
                match (acc, cur.status) {
                    // If currently requires update, nothing trumps this state
                    (PackageStatus::RequiresUpdate, _) => acc,
                    // Only requires update trumps NotInstalled
                    (PackageStatus::NotInstalled, PackageStatus::RequiresUpdate) => cur.status,
                    (PackageStatus::NotInstalled, PackageStatus::UpToDate) => {
                        PackageStatus::RequiresUpdate
                    }
                    (PackageStatus::UpToDate, PackageStatus::NotInstalled) => {
                        PackageStatus::RequiresUpdate
                    }
                    // Everything trumps UpToDate
                    (PackageStatus::UpToDate, v) => v,
                    _ => cur.status,
                }
            });
        let size = descriptors
            .iter()
            .fold(0, |acc, cur| acc + cur.release.target.payload.size());
        let installed_size = descriptors.iter().fold(0, |acc, cur| {
            acc + cur.release.target.payload.installed_size()
        });

        return ResolvedPackageQuery {
            descriptors,
            size,
            installed_size,
            status,
        };
    }

    // Everyone else gets an empty vec.
    ResolvedPackageQuery {
        descriptors: vec![],
        size: 0,
        installed_size: 0,
        status: PackageStatus::UpToDate,
    }
}

pub(crate) fn resolve_payload<'a>(
    package_key: &PackageKey,
    query: &ReleaseQuery<'a>,
    repos: &'a HashMap<RepoUrl, LoadedRepository>,
) -> Result<
    (
        pahkat_types::payload::Target,
        pahkat_types::package::Release,
        pahkat_types::package::Descriptor,
    ),
    PayloadError,
> {
    log::trace!("Resolving payload: {}", &package_key);
    let descriptor = resolve_package(package_key, repos)?;
    log::trace!("Package found: {}", &package_key);
    let result = query
        .iter(&descriptor)
        .next()
        .map(|x| (x.target.clone(), x.release.clone(), descriptor.clone()))
        .ok_or(PayloadError::NoPayloadFound);
    result
}

pub(crate) fn import<'a>(
    config: &Arc<RwLock<Config>>,
    package_key: &PackageKey,
    query: &ReleaseQuery<'a>,
    repos: &HashMap<RepoUrl, LoadedRepository>,
    installer_path: &Path,
) -> Result<std::path::PathBuf, crate::package_store::ImportError> {
    use pahkat_types::payload::AsDownloadUrl;
    log::debug!("IMPORTING");

    let (target, _, _) = resolve_payload(package_key, &query, &*repos)?;
    let config = config.read().unwrap();

    let output_path = download_file_path(&config, target.payload.as_download_url());
    log::debug!("DIR: {:?}", &installer_path);
    log::debug!("DIR: {:?}", &output_path);
    std::fs::create_dir_all(&output_path.parent().unwrap())?;
    std::fs::copy(installer_path, &output_path)?;
    Ok(output_path)
}
#[must_use]
pub(crate) fn download<'a>(
    config: &Arc<RwLock<Config>>,
    package_key: &PackageKey,
    query: &ReleaseQuery<'a>,
    repos: &HashMap<RepoUrl, LoadedRepository>,
) -> std::pin::Pin<
    Box<
        dyn futures::stream::Stream<Item = crate::package_store::DownloadEvent>
            + Send
            + Sync
            + 'static,
    >,
> {
    log::trace!("Downloading {} {:?}", package_key, &query);
    use pahkat_types::AsDownloadUrl;

    let (target, _, _) = match resolve_payload(package_key, &query, repos) {
        Ok(v) => v,
        Err(e) => {
            log::error!(
                "Failed to resolve: {} {:?}",
                &package_key,
                &query
            );
            return Box::pin(async_stream::stream! {
                yield crate::package_store::DownloadEvent::Error(crate::download::DownloadError::Payload(e));
            });
        }
    };

    let url = target.payload.as_download_url().to_owned();

    let config = config.read().unwrap();
    let settings = config.settings();
    let dm = crate::download::DownloadManager::new(
        settings.download_cache_dir().to_path_buf(),
        settings.max_concurrent_downloads(),
    );

    let output_path = crate::repo::download_dir(&*config, &url);
    let stream = async_stream::stream! {
        match dm.download(&url, output_path).await {
            Ok(mut v) => {
                while let Some(value) = v.next().await {
                    yield value;
                }
            }
            Err(e) => {
                yield DownloadEvent::Error(e);
            }
        }
    };
    Box::pin(stream)
}

pub(crate) fn download_dir(config: &Config, url: &url::Url) -> std::path::PathBuf {
    let mut sha = Sha256::new();
    sha.update(url.as_str().as_bytes());
    let hash_id = format!("{:x}", sha.finalize());
    let part1 = &hash_id[0..2];
    let part2 = &hash_id[2..4];
    let part3 = &hash_id[4..];

    config
        .settings()
        .package_cache_dir()
        .join(part1)
        .join(part2)
        .join(part3)
}

pub(crate) fn download_file_path(config: &Config, url: &url::Url) -> std::path::PathBuf {
    download_dir(config, url).join(
        url.path_segments()
            .unwrap_or_else(|| "".split('/'))
            .last()
            .unwrap(),
    )
}

pub(crate) fn all_statuses<'a>(
    store: &dyn PackageStore,
    repo_url: &RepoUrl,
    target: crate::package_store::InstallTarget,
) -> BTreeMap<String, Result<PackageStatus, PackageStatusError>> {
    log::debug!(
        "Getting all statuses for: {:?}, target: {:?}",
        repo_url,
        target
    );
    let mut map = BTreeMap::new();

    let repos = store.repos();
    let repos = repos.read().unwrap();

    if let Some(repo) = repos.get(repo_url) {
        let packages = repo.packages();
        let packages = match packages.packages() {
            Some(v) => v,
            None => {
                log::error!("No packages map in fbs for {:?}!", &repo_url);
                return map;
            }
        };

        for id in packages.keys() {
            let key =
                PackageKey::new_unchecked(repo.info().repository.url.clone(), id.to_string(), None);
            let status = store.status(&key, target);
            log::trace!("Package: {:?}, status: {:?}", &id, &status);
            map.insert(id.to_string(), status);
        }
    } else {
        log::warn!("Did not find repo {:?} in available repos", &repo_url);
        log::trace!("Repos available: {:?}", &*repos);
    }

    map
}

pub(crate) async fn strings<'p>(
    repo_urls: Vec<RepoUrl>,
    language: String,
) -> HashMap<RepoUrl, crate::package_store::LocalizedStrings> {
    let futures = repo_urls
        .into_iter()
        .map(|url| {
            let strings_url = url
                .join("strings/")
                .unwrap()
                .join(&format!("{}.toml", language))
                .unwrap();
            (url, strings_url)
        })
        .map(|(url, strings_url)| async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let response = match reqwest::get(strings_url).await {
                    Ok(v) => match v.text().await {
                        Ok(v) => match toml::from_str(&v) {
                            Ok(v) => Some(v),
                            Err(_) => None,
                        },
                        Err(_) => None,
                    },
                    Err(_) => None,
                };
                tx.send(response).unwrap();
            });
            let result = rx.await.unwrap();

            (url, result)
        })
        .collect::<Vec<_>>();
    let results = futures::future::join_all(futures).await;

    results
        .into_iter()
        .filter_map(|(k, v)| v.map(|v| (k, v)))
        .collect::<HashMap<_, _>>()
}

pub(crate) fn find_package_by_key<'p>(
    package_key: &PackageKey,
    repos: &'p HashMap<RepoUrl, LoadedRepository>,
) -> Option<Package> {
    log::trace!("Resolving package: {}", &package_key);
    log::trace!(
        "Available repos: {:?}",
        repos.iter().map(|(x, _)| x).collect::<Vec<_>>()
    );

    repos.get(&package_key.repository_url).and_then(|r| {
        log::trace!("Got repo: {}", &r.info.repository.url);
        // TODO: need to check that any release supports the requested channel
        let packages = r.packages();
        let packages = match packages.packages() {
            Some(v) => v,
            None => {
                log::error!(
                    "No packages map in fbs for {:?}!",
                    &package_key.repository_url
                );
                return None;
            }
        };

        let pkg = match packages.get(&package_key.id) {
            Some(x) => x,
            None => return None,
        };
        log::trace!("Found pkg: {}", &package_key);

        (&pkg).try_into().map(Package::Concrete).ok()
    })
}

pub(crate) fn find_package_by_id(
    store: &dyn PackageStore,
    package_id: &str,
    repos: &HashMap<RepoUrl, LoadedRepository>,
) -> Option<(PackageKey, Package)> {
    match PackageKey::try_from(package_id) {
        Ok(k) => return store.find_package_by_key(&k).map(|pkg| (k, pkg)),
        Err(_) => {}
    };

    repos.iter().find_map(|(key, repo)| {
        let packages = repo.packages();
        let packages = match packages.packages() {
            Some(v) => v,
            None => {
                log::error!("No packages map in fbs for {:?}!", &key);
                return None;
            }
        };

        packages.get(package_id).map(|x| {
            let key = PackageKey::new_unchecked(
                repo.info().repository.url.clone(),
                package_id.to_string(),
                None,
            );

            (&x).try_into().map(|p| (key, Package::Concrete(p))).ok()
        })?
    })
}

pub async fn work<T, W, F, C>(
    context: C,
    starting_items: SegQueue<W>,
    worker: F,
) -> Result<HashMap<W, T>, JoinError>
where
    W: std::fmt::Debug + Clone + Eq + Hash + Send + Sync + 'static,
    F: Fn(W, Arc<SegQueue<W>>, Arc<C>) -> Pin<Box<dyn Future<Output = T> + Send + Sync + 'static>>
        + Send
        + Sync
        + 'static,
    T: Send + Sync + 'static,
{
    let context = Arc::new(context);
    let work_queue = Arc::new(starting_items);
    let fut_queue = Arc::new(SegQueue::<(W, JoinHandle<T>)>::new());

    let mut is_processed = HashSet::<W>::new();
    let mut completed = HashMap::<W, T>::new();

    loop {
        if fut_queue.is_empty() && work_queue.is_empty() {
            log::trace!("All queues empty, breaking.");
            break;
        }

        if let Ok(item) = fut_queue.pop() {
            let (work, future) = item;
            log::trace!("Got item from future queue for {:?}.", &work);
            completed.insert(work, future.await?);
        }

        while let Ok(work) = work_queue.pop() {
            if is_processed.contains(&work) {
                log::trace!("Item {:?} already processed.", &work);
                continue;
            }

            is_processed.insert(work.clone());

            log::trace!("Processing {:?}", &work);
            fut_queue.push((
                work.clone(),
                tokio::spawn(worker(work, Arc::clone(&work_queue), Arc::clone(&context))),
            ));
        }
    }

    Ok(completed)
}

#[must_use]
pub(crate) async fn refresh_repos(
    config: Config,
) -> (
    HashMap<RepoUrl, LoadedRepository>,
    HashMap<RepoUrl, RepoDownloadError>,
) {
    let config = Arc::new(config);

    log::debug!("Refreshing repos...");

    let repo_data = {
        let repo_keys = config
            .repos()
            .keys()
            .fold(crossbeam_queue::SegQueue::new(), |acc, cur| {
                acc.push(cur.clone());
                acc
            });

        if repo_keys.is_empty() {
            Default::default()
        } else {
            work(config, repo_keys, |url, queue, config| {
                Box::pin(async move {
                    log::trace!("Downloading repo at {:?}…", &url);
    
                    let cache_dir = config.settings().repo_cache_dir();
                    let channel = config.repos().get(&url).and_then(|r| r.channel.clone());
    
                    match LoadedRepository::from_cache_or_url(url, channel, cache_dir).await {
                        Ok(repo) => {
                            for url in repo.info().repository.linked_repositories.iter() {
                                log::trace!("Queuing linked repo: {:?}", &url);
                                queue.push(url.clone());
                                // recurse_repo(url.clone(), Arc::clone(&repos), Arc::clone(&config)).await?;
                            }
    
                            Ok(repo)
                        }
                        Err(e) => {
                            log::error!("{:?}", e);
                            Err(e)
                        }
                    }
                })
            })
            .await
            .unwrap()
        }
    };

    let mut res_map = HashMap::new();
    let mut err_map = HashMap::new();

    for (key, value) in repo_data.into_iter() {
        match value {
            Ok(v) => {
                log::debug!("Resolved repository: {:?}", &key);
                res_map.insert(key, v);
            }
            Err(e) => {
                log::debug!("Repository resolution failed: {:?} {:?}", &key, &e);
                err_map.insert(key, e);
            }
        }
    }

    log::trace!("Finished refreshing");

    (res_map, err_map)
}

pub(crate) fn clear_cache(config: &Arc<RwLock<Config>>) {
    // todo!()
}

#[derive(Debug, Clone)]
pub struct PackageCandidate {
    pub package_key: PackageKey,
    pub action: PackageActionType,
    pub descriptor: Descriptor,
    pub release: Release,
    pub target: Target,
    pub status: PackageStatus,
    pub is_reboot_required: bool,
}

impl PackageCandidate {
    fn dependencies_in_set(&self, set: &[PackageCandidate]) -> usize {
        self.target
            .dependencies
            .keys()
            .filter(|x| {
                let v = x.to_package_key(&self.package_key.repository_url);
                let key = match v {
                    Ok(v) => v,
                    Err(_) => return false,
                };

                set.iter().find(|x| x.package_key == key).is_some()
            })
            .count()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PackageCandidateError {
    #[error("Could not resolve package status for package key: `{0}`")]
    Status(PackageKey, #[source] PackageStatusError),

    #[error("Could not resolve payload for package key: `{0}`")]
    Payload(PackageKey, #[source] PayloadError),

    #[error("Could not resolve identifier to package key: `{0}`")]
    UnresolvedId(String),

    #[error("Attempting to uninstall package required by installation set: `{0}`")]
    UninstallConflict(PackageKey),
}

use crate::{ext::DependencyKeyExt, package_store::InstallTarget, PackageActionType};
use types::DependencyKey;

fn resolve_package_candidate(
    store: &dyn PackageStore,
    candidate: &(PackageActionType, PackageKey),
    install_target: &[InstallTarget],
    repos: &HashMap<RepoUrl, LoadedRepository>,
) -> Result<PackageCandidate, PackageCandidateError> {
    let package_key = &candidate.1;
    let query = crate::repo::ReleaseQuery::new(package_key, &repos);

    match candidate.0 {
        PackageActionType::Install => {
            let status = install_target
                .iter()
                .fold(None, |acc, cur| match acc {
                    Some(Ok(v)) if v != PackageStatus::NotInstalled => Some(Ok(v)),
                    _ => Some(
                        store
                            .status(&package_key, *cur)
                            .map_err(|e| PackageCandidateError::Status(package_key.to_owned(), e)),
                    ),
                })
                .unwrap_or_else(|| {
                    Err(PackageCandidateError::UnresolvedId(package_key.to_string()))
                })?;

            let (target, release, descriptor) = resolve_payload(package_key, &query, &*repos)
                .map_err(|e| PackageCandidateError::Payload(package_key.to_owned(), e))?;

            use pahkat_types::payload::Payload;

            let is_reboot_required = match &target.payload {
                Payload::TarballPackage(_) => false,
                Payload::MacOSPackage(pkg) => {
                    use pahkat_types::payload::macos::RebootSpec;
                    match status {
                        PackageStatus::NotInstalled => {
                            pkg.requires_reboot.contains(&RebootSpec::Install)
                        }
                        PackageStatus::RequiresUpdate => {
                            pkg.requires_reboot.contains(&RebootSpec::Update)
                        }
                        _ => false,
                    }
                }
                Payload::WindowsExecutable(pkg) => {
                    use pahkat_types::payload::windows::RebootSpec;
                    match status {
                        PackageStatus::NotInstalled => {
                            pkg.requires_reboot.contains(&RebootSpec::Install)
                        }
                        PackageStatus::RequiresUpdate => {
                            pkg.requires_reboot.contains(&RebootSpec::Update)
                        }
                        _ => false,
                    }
                }
                _ => false,
            };

            Ok(PackageCandidate {
                package_key: package_key.to_owned(),
                action: candidate.0,
                descriptor,
                release,
                target,
                status,
                is_reboot_required,
            })
        }
        PackageActionType::Uninstall => {
            let status = install_target
                .iter()
                .fold(None, |acc, cur| match acc {
                    Some(Ok(v)) if v != PackageStatus::NotInstalled => Some(Ok(v)),
                    _ => Some(
                        store
                            .status(&package_key, *cur)
                            .map_err(|e| PackageCandidateError::Status(package_key.to_owned(), e)),
                    ),
                })
                .unwrap_or_else(|| {
                    Err(PackageCandidateError::UnresolvedId(package_key.to_string()))
                })?;

            let (target, release, descriptor) = resolve_payload(package_key, &query, &*repos)
                .map_err(|e| PackageCandidateError::Payload(package_key.to_owned(), e))?;

            use pahkat_types::payload::Payload;

            let is_reboot_required = match &target.payload {
                Payload::TarballPackage(_) => false,
                Payload::MacOSPackage(pkg) => {
                    use pahkat_types::payload::macos::RebootSpec;
                    match status {
                        PackageStatus::NotInstalled => {
                            pkg.requires_reboot.contains(&RebootSpec::Uninstall)
                        }
                        _ => false,
                    }
                }
                Payload::WindowsExecutable(pkg) => {
                    use pahkat_types::payload::windows::RebootSpec;
                    match status {
                        PackageStatus::NotInstalled => {
                            pkg.requires_reboot.contains(&RebootSpec::Uninstall)
                        }
                        _ => false,
                    }
                }
                _ => false,
            };

            Ok(PackageCandidate {
                package_key: package_key.to_owned(),
                action: candidate.0,
                descriptor,
                release,
                target,
                status,
                is_reboot_required,
            })
        }
    }
}

fn recurse_package_set(
    store: &dyn PackageStore,
    package_candidate: &PackageCandidate,
    install_target: &[InstallTarget],
    repos: &HashMap<RepoUrl, LoadedRepository>,
    set: &mut HashMap<PackageKey, PackageCandidate>,
) -> Result<(), PackageCandidateError> {
    package_candidate
        .target
        .dependencies
        .keys()
        .try_fold((), |_, key| {
            let key = match key {
                DependencyKey::Remote(key) => PackageKey::try_from(key)
                    .map_err(|_| PackageCandidateError::UnresolvedId(key.to_string()))?,
                DependencyKey::Local(key) => store
                    .find_package_by_id(key)
                    .map(|x| x.0)
                    .ok_or_else(|| PackageCandidateError::UnresolvedId(key.to_string()))?,
            };

            // FIXME: this uninstall thing here is a workaround to make uninstall work at all.
            // No dependency cleanup will occur.
            if set.contains_key(&key) || package_candidate.action == PackageActionType::Uninstall {
                return Ok(());
            }

            let candidate = resolve_package_candidate(
                store,
                &(PackageActionType::Install, key.to_owned()),
                install_target,
                repos,
            )?;
            set.insert(key, candidate);
            Ok(())
        })
}

pub(crate) fn resolve_package_set(
    store: &dyn PackageStore,
    candidates: &[(PackageActionType, PackageKey)],
    install_target: &[InstallTarget],
) -> Result<Vec<PackageCandidate>, PackageCandidateError> {
    let repos = store.repos();
    let repos = repos.read().unwrap();

    // Resolve initial package set
    let mut candidate_set = candidates
        .iter()
        .map(|key| {
            resolve_package_candidate(store, &key, install_target, &*repos)
                .map(|v| (key.1.to_owned(), v))
        })
        .collect::<Result<HashMap<_, _>, _>>()?;

    // Iterate all dependencies until we achieve victory
    let values = candidate_set.values().cloned().collect::<Vec<_>>();
    log::trace!("Package candidates: {:?}", &values);

    values.iter().try_fold((), |_, candidate| {
        log::trace!("Recursing packages for candidate: {:?}", candidate);

        recurse_package_set(
            store,
            candidate,
            install_target,
            &*repos,
            &mut candidate_set,
        )
    })?;

    // Take our candidate set and resolve it down to a mutation set
    let mutation_set: Vec<PackageCandidate> = candidate_set
        .into_iter()
        .filter_map(|(key, candidate)| {
            if candidate.action == PackageActionType::Install
                && candidate.status == PackageStatus::UpToDate
            {
                None
            } else if candidate.action == PackageActionType::Uninstall
                && candidate.status == PackageStatus::NotInstalled
            {
                None
            } else {
                Some(candidate)
            }
        })
        .collect();

    let mut output_mutation_set = mutation_set.clone();

    // WORKAROUND: re-order these dependencies so that they are in dependency order
    output_mutation_set.sort_by(|a, b| {
        let a_deps = a.dependencies_in_set(&mutation_set);
        let b_deps = b.dependencies_in_set(&mutation_set);
        a_deps.cmp(&b_deps)
    });

    log::trace!(
        "Output mutation set: {:?}",
        &output_mutation_set
            .iter()
            .map(|x| x.package_key.to_string())
            .collect::<Vec<_>>()
    );

    Ok(output_mutation_set)
}
