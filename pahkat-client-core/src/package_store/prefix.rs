#![cfg(feature = "prefix")]

use std::{collections::BTreeMap, unreachable};
use std::fs::{create_dir_all, read_dir, remove_dir, remove_file, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use hashbrown::HashMap;
use pahkat_types::package::{Descriptor, Package};
use pahkat_types::repo::RepoUrl;
use r2d2_sqlite::SqliteConnectionManager;
use xz2::bufread::XzDecoder;

use super::InstallTarget;
use crate::{PackageActionType, package_store::{SharedRepoErrors, SharedRepos, SharedStoreConfig}, repo::PackageCandidateError};
use crate::repo::RepoDownloadError;
use crate::transaction::{
    install::InstallError, uninstall::UninstallError, PackageDependencyError, ResolvedPackageQuery, PackageDependencyStatusError,
};
use crate::{
    cmp,
    download::Download,
    download::DownloadManager,
    ext::DependencyKeyExt,
    package_store::ImportError,
    repo::{LoadedRepository, PackageQuery},
    transaction::PackageStatusError,
    transaction::{PackageStatus, ResolvedDescriptor},
    Config, PackageKey, PackageStore,
};

// type Result<T> = std::result::Result<T, Error>;

const SQL_INIT: &str = include_str!("prefix/prefix_init.sql");

pub struct PrefixPackageStore {
    pool: r2d2::Pool<SqliteConnectionManager>,
    prefix: PathBuf,
    repos: SharedRepos,
    errors: SharedRepoErrors,
    config: SharedStoreConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Provided path was not a valid prefix destination")]
    InvalidPrefixPath(#[source] std::io::Error),

    #[error("Create directory failed")]
    CreateDirFailed(#[source] std::io::Error),

    #[error("Error creating or loading config")]
    Config(#[from] crate::config::Error),

    #[error("Error connecting to database")]
    DatabaseConnection(#[from] r2d2::Error),

    #[error("Error processing SQL query")]
    Database(#[from] rusqlite::Error),
}

impl PrefixPackageStore {
    pub async fn open_or_create<P: AsRef<Path>>(
        prefix_path: P,
    ) -> Result<PrefixPackageStore, Error> {
        match Self::open(prefix_path.as_ref()).await {
            Ok(v) => return Ok(v),
            Err(e) => match e {
                Error::InvalidPrefixPath(_) => {}
                e => return Err(e),
            },
        };

        Self::create(prefix_path).await
    }

    pub async fn create<P: AsRef<Path>>(prefix_path: P) -> Result<PrefixPackageStore, Error> {
        create_dir_all(&prefix_path).map_err(Error::CreateDirFailed)?;
        let prefix_path = prefix_path
            .as_ref()
            .canonicalize()
            .map_err(Error::InvalidPrefixPath)?;
        create_dir_all(&prefix_path.join("pkg")).map_err(Error::CreateDirFailed)?;

        let (config, errors) = Config::load(&prefix_path, crate::config::Permission::ReadWrite);
        // TODO: handle errors

        let db_file_path = PrefixPackageStore::package_db_path(&config);
        let manager = SqliteConnectionManager::file(&db_file_path);
        let pool = Self::make_pool(manager)?;
        let conn = pool.get()?;
        conn.execute_batch(SQL_INIT)?;

        let store = PrefixPackageStore {
            pool,
            prefix: prefix_path,
            repos: Default::default(),
            errors: Default::default(),
            config: Arc::new(RwLock::new(config)),
        };

        // We ignore failures here.
        let _ = store.refresh_repos().await;

        Ok(store)
    }

    pub async fn open<P: AsRef<Path>>(prefix_path: P) -> Result<PrefixPackageStore, Error> {
        let prefix_path = prefix_path
            .as_ref()
            .canonicalize()
            .map_err(Error::InvalidPrefixPath)?;
        log::debug!("{:?}", &prefix_path);
        let (config, errors) = Config::load(&prefix_path, crate::config::Permission::ReadWrite);


        let db_file_path = PrefixPackageStore::package_db_path(&config);
        log::debug!("{:?}", &db_file_path);
        let manager = SqliteConnectionManager::file(&db_file_path);
        let pool = Self::make_pool(manager)?;

        let store = PrefixPackageStore {
            pool,
            prefix: prefix_path,
            repos: Default::default(),
            errors: Default::default(),
            config: Arc::new(RwLock::new(config)),
        };

        // We ignore failures here.
        let _ = store.refresh_repos().await;

        Ok(store)
    }

    #[inline(always)]
    fn make_pool(
        manager: SqliteConnectionManager,
    ) -> Result<r2d2::Pool<SqliteConnectionManager>, r2d2::Error> {
        r2d2::Pool::builder()
            .max_size(4)
            .min_idle(Some(0))
            .idle_timeout(Some(std::time::Duration::new(10, 0)))
            .build(manager)
    }

    fn package_db_path(config: &Config) -> PathBuf {
        config.settings().config_dir().join("packages.sqlite")
    }

    fn package_dir(&self, package_id: &str) -> PathBuf {
        self.prefix.join("pkg").join(package_id)
    }
}

/// <script>
/// (function() {
/// var s = document.currentScript
/// window.addEventListener('DOMContentLoaded', function(evt) {
/// var docblock = s.parentNode
/// docblock.classList.remove("hidden-by-usual-hider")
/// docblock.nextSibling.classList.remove("fns-now-collapsed")
/// var l = docblock.nextSibling.children.length
/// for (var i = 0; i < l; ++i) {
///   var cl = docblock.nextSibling.children[i].classList;
///   cl.remove("collapsed")
///   cl.remove("hidden")
///   cl.remove("hidden-default")
///   cl.remove("hidden-by-impl-hider")
///   cl.add("x")
/// }
/// })
/// })()
/// </script>
impl PackageStore for PrefixPackageStore {
    fn repos(&self) -> super::SharedRepos {
        Arc::clone(&self.repos)
    }

    fn errors(&self) -> super::SharedRepoErrors {
        Arc::clone(&self.errors)
    }

    fn config(&self) -> super::SharedStoreConfig {
        Arc::clone(&self.config)
    }

    fn import(&self, key: &PackageKey, installer_path: &Path) -> Result<PathBuf, ImportError> {
        log::debug!("IMPORTING");
        let repos = self.repos.read().unwrap();
        let query = crate::repo::ReleaseQuery::new(key, &*repos);
        crate::repo::import(&self.config, key, &query, &*repos, installer_path)
    }

    fn download(
        &self,
        key: &PackageKey,
    ) -> std::pin::Pin<
        Box<
            dyn futures::stream::Stream<Item = crate::package_store::DownloadEvent>
                + Send
                + 'static,
        >,
    > {
        let repos = self.repos.read().unwrap();
        let query = crate::repo::ReleaseQuery::new(key, &*repos);
        crate::repo::download(&self.config, key, &query, &*repos)
    }

    fn install(
        &self,
        key: &PackageKey,
        target: InstallTarget,
    ) -> Result<PackageStatus, InstallError> {
        log::trace!("In prefix install");

        let repos = self.repos.read().unwrap();
        let query = crate::repo::ReleaseQuery::new(key, &*repos);

        log::trace!("Query: {:?}", &query);
        let (target, release, package) =
            crate::repo::resolve_payload(key, &query, &*repos).map_err(InstallError::Payload)?;
        let installer = match target.payload {
            pahkat_types::payload::Payload::TarballPackage(v) => v,
            _ => return Err(InstallError::WrongPayloadType),
        };
        let pkg_path =
            crate::repo::download_file_path(&*self.config.read().unwrap(), &installer.url);
        log::debug!("Installing {}: {:?}", &key, &pkg_path);

        if !pkg_path.exists() {
            log::error!("Package path doesn't exist: {:?}", &pkg_path);
            return Err(InstallError::PackageNotInCache);
        }

        let file = File::open(&pkg_path).unwrap();
        let reader = XzDecoder::new(std::io::BufReader::new(file));

        let mut tar_file = tar::Archive::new(reader);
        let mut files = vec![];

        let pkg_path = self.package_dir(&package.package.id);
        create_dir_all(&pkg_path).unwrap(); // map_err(InstallError::CreateDirFailed)?;

        log::debug!("Prefix: {:?}", &self.prefix);

        for entry in tar_file.entries().unwrap() {
            let mut entry = entry.unwrap();
            let unpack_res;
            {
                unpack_res = entry.unpack_in(&pkg_path).unwrap(); //.context(UnpackFailed)?;
            }

            if unpack_res {
                let entry_path = entry.header().path().unwrap();
                log::debug!("entry path: {:?}", &entry_path);
                let entry_path = entry_path.to_str().unwrap().to_string();
                files.push(entry_path);
            } else {
                continue;
            }
        }

        let deps = &target.dependencies;
        let dependencies: Vec<String> = deps
            .keys()
            .map(|x| x.to_package_key(&key.repository_url).unwrap().to_string())
            .collect();

        {
            let record = PackageDbRecord {
                id: 0,
                url: key.clone().without_query_params().to_string(),
                version: release.version.to_string(),
                files,
                dependencies,
            };

            let mut conn = self.pool.get().unwrap();
            record.save(&mut conn).unwrap();
        };

        Ok(PackageStatus::UpToDate)
    }

    fn uninstall(
        &self,
        key: &PackageKey,
        _target: InstallTarget,
    ) -> Result<PackageStatus, UninstallError> {
        let mut conn = self.pool.get().unwrap();
        let record = match PackageDbRecord::find_by_id(&mut conn, &key) {
            None => return Err(UninstallError::NotInstalled),
            Some(v) => v,
        };

        let pkg_path = self.package_dir(&key.id);
        for file in &record.files {
            let file = match pkg_path.join(file).canonicalize() {
                Ok(v) => v,
                Err(_) => continue,
            };

            if file.is_dir() {
                continue;
            }

            if file.exists() {
                remove_file(file).unwrap();
            }
        }

        for file in &record.files {
            let file = match pkg_path.join(file).canonicalize() {
                Ok(v) => v,
                Err(_) => continue,
            };

            if !file.is_dir() {
                continue;
            }

            let dir = read_dir(&file).unwrap();
            if dir.count() == 0 {
                remove_dir(&file).unwrap();
            }
        }

        record.delete(&mut conn).unwrap();

        Ok(PackageStatus::NotInstalled)
    }

    fn status(
        &self,
        key: &PackageKey,
        _target: InstallTarget,
    ) -> Result<PackageStatus, PackageStatusError> {
        let mut conn = self.pool.get().unwrap();
        let record = match PackageDbRecord::find_by_id(&mut conn, &key) {
            None => return Ok(PackageStatus::NotInstalled),
            Some(v) => v,
        };

        let repos = self.repos.read().unwrap();
        let query =
            crate::repo::ReleaseQuery::new(key, &*repos).and_payloads(vec!["TarballPackage"]);
        log::debug!("query: {:?}", &query);

        let (target, release, package) = crate::repo::resolve_payload(key, &query, &*repos)
            .map_err(PackageStatusError::Payload)?;
        let _installer = match target.payload {
            pahkat_types::payload::Payload::TarballPackage(v) => v,
            _ => return Err(PackageStatusError::WrongPayloadType),
        };

        let config = self.config.read().unwrap();
        let status = self::cmp::cmp(&record.version, &release.version);

        log::debug!("Status: {:?}", &status);
        status
    }

    fn dependency_status(
        &self,
        key: &PackageKey,
        target: InstallTarget,
    ) -> Result<Vec<(PackageKey, PackageStatus)>, PackageDependencyStatusError> {
        crate::repo::resolve_package_set(self, &[(PackageActionType::Install, key.clone())], &[target])
            .map(|dep| dep.into_iter().map(|dep| (dep.package_key, dep.status)).collect())
            .map_err(|err| match err {
                PackageCandidateError::Status(p, PackageStatusError::Payload(e)) => PackageDependencyStatusError::Payload(p, e),
                PackageCandidateError::Status(p, PackageStatusError::WrongPayloadType) => PackageDependencyStatusError::WrongPayloadType(p),
                PackageCandidateError::Status(p, PackageStatusError::ParsingVersion) => PackageDependencyStatusError::ParsingVersion(p),

                PackageCandidateError::Payload(p, e) => PackageDependencyStatusError::Payload(p, e),
                PackageCandidateError::UnresolvedId(id) => PackageDependencyStatusError::PackageNotFound(id),
                PackageCandidateError::UninstallConflict(_) => unreachable!()
            })
    }

    fn all_statuses(
        &self,
        repo_url: &RepoUrl,
        target: InstallTarget,
    ) -> BTreeMap<String, Result<PackageStatus, PackageStatusError>> {
        crate::repo::all_statuses(self, repo_url, target)
    }

    fn find_package_by_key(&self, key: &PackageKey) -> Option<Package> {
        let repos = self.repos.read().unwrap();
        crate::repo::find_package_by_key(key, &*repos)
    }

    fn find_package_by_id(&self, package_id: &str) -> Option<(PackageKey, Package)> {
        let repos = self.repos.read().unwrap();
        crate::repo::find_package_by_id(self, package_id, &*repos)
    }

    fn refresh_repos(
        &self,
    ) -> crate::package_store::Future<Result<(), HashMap<RepoUrl, RepoDownloadError>>> {
        let config = self.config().read().unwrap().clone();
        let repos = self.repos();
        Box::pin(async move {
            log::trace!("Calling into refresh repos");
            let (result, errors) = crate::repo::refresh_repos(config).await;
            log::trace!("Finished refresh repos: {:?}", &errors);
            *repos.write().unwrap() = result;
            if errors.is_empty() {
                Ok(())
            } else {
                Err(errors)
            }
        })
    }

    fn clear_cache(&self) {
        crate::repo::clear_cache(&self.config)
    }

    fn strings(
        &self,
        language: String,
    ) -> crate::package_store::Future<HashMap<RepoUrl, crate::package_store::LocalizedStrings>>
    {
        let repos = self.repos.read().unwrap();
        let urls = repos.keys().cloned().collect::<Vec<_>>();

        Box::pin(crate::repo::strings(urls, language))
    }

    fn resolve_package_query(
        &self,
        query: PackageQuery,
        install_target: &[InstallTarget],
    ) -> ResolvedPackageQuery {
        let repos = self.repos();
        let repos = repos.read().unwrap();
        crate::repo::resolve_package_query(self, &query, install_target, &*repos)
    }
}

#[derive(Debug)]
struct PackageDbRecord {
    id: i64,
    url: String,
    version: String,
    files: Vec<String>,
    dependencies: Vec<String>,
}

struct PackageDbConnection<'a>(&'a mut rusqlite::Connection);

impl<'a> PackageDbConnection<'a> {
    fn dependencies(&self, url: &str) -> Vec<String> {
        let mut stmt = self
            .0
            .prepare("SELECT dependency_id FROM packages_dependencies WHERE package_id = (SELECT id FROM packages WHERE url = ?)")
            .unwrap();

        let res = stmt
            .query_map(&[&url], |row| row.get(0))
            .unwrap()
            .map(|x| x.unwrap())
            .collect();

        res
    }

    fn files(&self, url: &str) -> Vec<String> {
        let mut stmt = self
            .0
            .prepare("SELECT file_path FROM packages_files WHERE package_id = (SELECT id FROM packages WHERE url = ?)")
            .expect("prepared statement");

        let res = stmt
            .query_map(&[&url], |row| row.get(0))
            .expect("query_map succeeds")
            .map(|x: Result<String, _>| x.unwrap())
            .collect();

        res
    }

    fn version(&self, url: &str) -> Option<String> {
        match self.0.query_row(
            "SELECT version FROM packages WHERE url = ? LIMIT 1",
            &[&url],
            |row| row.get(0),
        ) {
            Ok(v) => v,
            Err(_) => return None,
        }
    }

    fn replace_pkg(&mut self, pkg: &PackageDbRecord) -> rusqlite::Result<()> {
        use chrono::prelude::*;
        let utc: DateTime<Utc> = Utc::now();
        let utc = format!("{:?}", utc);

        let tx = self.0.transaction().unwrap();

        tx.execute_named(
            "INSERT INTO packages(url, version, installed_on, updated_on)
            VALUES (:url, :version, :installed_on, :updated_on)
            ON CONFLICT(url) DO UPDATE SET
                version=excluded.version,
                updated_on=excluded.updated_on",
            &[
                // (":id", &pkg.id),
                (":url", &pkg.url),
                (":version", &pkg.version),
                (":installed_on", &utc),
                (":updated_on", &utc),
            ],
        )
        .unwrap();
        let id: i64;
        {
            let mut stmt = tx
                .prepare("SELECT id FROM packages WHERE url = :url")
                .unwrap();
            let mut rows = stmt.query_named(&[(":url", &pkg.url)]).unwrap();
            id = rows.next().unwrap().unwrap().get(0).unwrap()
        }

        log::trace!("Row id: {}", id);
        tx.execute(
            "DELETE FROM packages_dependencies WHERE package_id = ?",
            &[id],
        )
        .unwrap();
        tx.execute("DELETE FROM packages_files WHERE package_id = ?", &[id])
            .unwrap();

        {
            let mut dep_stmt = tx
                .prepare(
                    "INSERT INTO packages_dependencies(package_id, dependency_id) VALUES (:id, (SELECT id FROM packages WHERE url = :dep_url))",
                )
                .unwrap();
            for dep_url in &pkg.dependencies {
                dep_stmt.execute_named(&[(":id", &id), (":dep_url", &*dep_url)])?;
            }

            let mut file_stmt = tx
                .prepare("INSERT INTO packages_files(package_id, file_path) VALUES (:id, :path)")?;

            for file_path in &pkg.files {
                file_stmt
                    .execute_named(&[(":id", &id), (":path", &file_path.as_str())])
                    .unwrap();
            }
        }

        tx.commit()
    }

    fn remove_pkg(&mut self, pkg: &PackageDbRecord) -> rusqlite::Result<()> {
        let tx = self.0.transaction().unwrap();

        tx.execute("DELETE FROM packages WHERE id = ?", &[&pkg.id])?;
        tx.execute(
            "DELETE FROM packages_dependencies WHERE package_id = ?",
            &[&pkg.id],
        )?;
        tx.execute(
            "DELETE FROM packages_files WHERE package_id = ?",
            &[&pkg.id],
        )?;

        tx.commit()
    }
}

impl PackageDbRecord {
    pub fn find_by_id(
        conn: &mut rusqlite::Connection,
        key: &PackageKey,
    ) -> Option<PackageDbRecord> {
        let conn = PackageDbConnection(conn);
        let url = key.clone().without_query_params().to_string();

        let version = match conn.version(&url) {
            Some(v) => v,
            None => return None,
        };

        let files = conn.files(&url);
        let dependencies = conn.dependencies(&url);

        Some(PackageDbRecord {
            id: 0,
            url,
            version,
            files,
            dependencies,
        })
    }

    pub fn save(&self, conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
        log::trace!("Saving package record: {:?}", &self);
        PackageDbConnection(conn).replace_pkg(self)
    }

    pub fn delete(self, conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
        log::trace!("Deleting package record: {:?}", &self);
        PackageDbConnection(conn).remove_pkg(&self)
    }
}

// #[test]
// fn test_create_database() {
//     let conn = rusqlite::Connection::open_in_memory().unwrap();
//     let store = TarballPackageStore::new(conn, Path::new("/"));
//     store.init_sqlite_database().unwrap();
// }

// #[test]
// fn test_create_record() {
//     let mut conn = {
//         let mut conn = rusqlite::Connection::open_in_memory().unwrap();
//         let store = TarballPackageStore::new(conn, Path::new("/"));
//         store.init_sqlite_database().unwrap();
//         store.conn.into_inner()
//     };

//     let pkg = PackageDbRecord {
//         id: "test-pkg".to_owned(),
//         version: "1.0.0".to_owned(),
//         files: vec![Path::new("bin/test").to_path_buf()],
//         dependencies: vec!["one-thing".to_owned()]
//     };

//     {
//         pkg.save(conn.transaction().unwrap()).unwrap();
//     }

//     let found = PackageDbRecord::find_by_id(&mut conn, "test-pkg");
//     assert!(found.is_some());
// }

// #[test]
// fn test_download_repo() {
//     let repo = download_repository("http://localhost:8000").unwrap();
//     assert_eq!(repo.meta.base, "localhost");
// }

// #[test]
// fn test_extract_files() {
//     let tmpdir = Path::new("/tmp");
//     let conn = rusqlite::Connection::open_in_memory().unwrap();

//     let pkgstore = TarballPackageStore::new(conn, tmpdir);
//     pkgstore.init_sqlite_database();
//     let repo = Repository::from_url("http://localhost:8000").unwrap();

//     let test_pkg = repo.package("test-pkg").unwrap();
//     let inst_path = test_pkg.download(&tmpdir).expect("a download");
//     pkgstore.install(test_pkg, &inst_path).unwrap();
//     pkgstore.uninstall(test_pkg).unwrap();
// }
