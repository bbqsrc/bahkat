#![cfg(windows)]
use super::{Repository, StoreConfig};
use crate::transaction::PackageTransaction;
use crate::transaction::{PackageStatus, PackageStatusError};
use crate::*;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use pahkat_types::{Downloadable, InstallTarget, Installer, Package, WindowsInstaller};
use semver;
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{self, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use url;
use winreg::enums::*;
use winreg::RegKey;

mod sys {
    use std::ffi::{OsStr, OsString};
    use std::ops::Range;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::ffi::OsStringExt;
    use std::slice;
    use winapi::ctypes::c_void;
    use winapi::um::shellapi::CommandLineToArgvW;
    use winapi::um::winbase::LocalFree;

    // https://github.com/rust-lang/rust/blob/f76d9bcfc2c269452522fbbe19f66fe653325646/src/libstd/sys/windows/os.rs#L286-L289
    pub struct Args {
        range: Range<isize>,
        cur: *mut *mut u16,
    }

    impl Iterator for Args {
        type Item = OsString;
        fn next(&mut self) -> Option<OsString> {
            self.range.next().map(|i| unsafe {
                let ptr = *self.cur.offset(i);
                let mut len = 0;
                while *ptr.offset(len) != 0 {
                    len += 1;
                }

                // Push it onto the list.
                let ptr = ptr as *const u16;
                let buf = slice::from_raw_parts(ptr, len as usize);
                OsStringExt::from_wide(buf)
            })
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            self.range.size_hint()
        }
    }

    impl ExactSizeIterator for Args {
        fn len(&self) -> usize {
            self.range.len()
        }
    }

    impl Drop for Args {
        fn drop(&mut self) {
            unsafe {
                LocalFree(self.cur as *mut c_void);
            }
        }
    }

    pub fn args<S: AsRef<OsStr>>(input: S) -> Args {
        let input_vec: Vec<u16> = OsStr::new(&input)
            .encode_wide()
            .chain(Some(0).into_iter())
            .collect();
        let lp_cmd_line = input_vec.as_ptr();
        let mut args: i32 = 0;
        let arg_list: *mut *mut u16 = unsafe { CommandLineToArgvW(lp_cmd_line, &mut args) };
        Args {
            range: 0..(args as isize),
            cur: arg_list,
        }
    }
}

mod Keys {
    pub const UninstallPath: &'static str = r"Software\Microsoft\Windows\CurrentVersion\Uninstall";
    pub const DisplayVersion: &'static str = "DisplayVersion";
    pub const SkipVersion: &'static str = "SkipVersion";
    pub const QuietUninstallString: &'static str = "QuietUninstallString";
    pub const UninstallString: &'static str = "UninstallString";
}

type SharedStoreConfig = Arc<RwLock<StoreConfig>>;
type SharedRepos = Arc<RwLock<HashMap<RepoRecord, Repository>>>;

#[derive(Debug)]
pub struct WindowsPackageStore {
    repos: SharedRepos,
    config: SharedStoreConfig,
}

fn installer(package: &Package) -> Result<&WindowsInstaller, PackageStatusError> {
    match package.installer() {
        None => Err(PackageStatusError::NoInstaller),
        Some(v) => match v {
            &Installer::Windows(ref v) => Ok(v),
            _ => Err(PackageStatusError::WrongInstallerType),
        },
    }
}

fn uninstall_regkey(installer: &WindowsInstaller) -> Option<RegKey> {
    let hklm = RegKey::predef(HKEY_LOCAL_MACHINE);
    let path = Path::new(Keys::UninstallPath).join(&installer.product_code);
    match hklm.open_subkey(&path) {
        Err(e) => match hklm.open_subkey_with_flags(&path, KEY_READ | KEY_WOW64_64KEY) {
            Err(e) => None,
            Ok(v) => Some(v),
        },
        Ok(v) => Some(v),
    }
}

use crate::transaction::PackageStore;

use crate::transaction::{
    install::InstallError, install::InvalidUrl, install::ProcessError, uninstall::UninstallError,
    PackageActionType, PackageDependencyError,
};

impl PackageStore for WindowsPackageStore {
    type Target = InstallTarget;

    fn repos(&self) -> SharedRepos {
        Arc::clone(&self.repos)
    }

    fn download(
        &self,
        key: &PackageKey,
        progress: Box<dyn Fn(u64, u64) -> () + Send + 'static>,
    ) -> Result<PathBuf, crate::download::DownloadError> {
        let package = match self.resolve_package(key) {
            Some(v) => v,
            None => {
                return Err(crate::download::DownloadError::NoUrl);
            }
        };

        let installer = match package.installer() {
            None => return Err(crate::download::DownloadError::NoUrl),
            Some(v) => v,
        };

        let config = &self.config.read().unwrap();

        let download_path = crate::repo::download_path(config, &installer.url());
        let tmp_path = config.tmp_path().to_path_buf();
        let disposable = package.download(tmp_path, &download_path, Some(progress))?;
        disposable.wait()
    }

    fn install(
        &self,
        key: &PackageKey,
        target: &Self::Target,
    ) -> Result<PackageStatus, InstallError> {
        let package = match self.resolve_package(key) {
            Some(v) => v,
            None => {
                return Err(InstallError::NoPackage);
            }
        };

        let installer = match package.installer() {
            None => return Err(InstallError::NoInstaller),
            Some(v) => v,
        };

        let installer = match *installer {
            Installer::Windows(ref v) => v,
            _ => return Err(InstallError::WrongInstallerType),
        };

        let url = url::Url::parse(&installer.url).map_err(|source| InstallError::InvalidUrl {
            source,
            url: installer.url.to_owned(),
        })?;
        let filename = url.path_segments().unwrap().last().unwrap();
        let pkg_path =
            crate::repo::download_path(&self.config.read().unwrap(), &url.as_str()).join(filename);

        if !pkg_path.exists() {
            return Err(InstallError::PackageNotInCache);
        }

        let mut args: Vec<OsString> = match (&installer.installer_type, &installer.args) {
            (_, &Some(ref v)) => sys::args(&v).map(|x| x.clone()).collect(),
            (&Some(ref type_), &None) => {
                let mut arg_str = OsString::new();
                // TODO: generic parameter extensions for windows based on install target
                match type_.as_ref() {
                    "inno" => {
                        arg_str.push("\"");
                        arg_str.push(&pkg_path);
                        arg_str.push("\" /VERYSILENT /SP- /SUPPRESSMSGBOXES /NORESTART");
                        // TODO: add user-mode installation?
                    }
                    "msi" => {
                        arg_str.push("msiexec /i \"");
                        arg_str.push(&pkg_path);
                        arg_str.push("\" /qn /norestart");
                    }
                    "nsis" => {
                        arg_str.push("\"");
                        arg_str.push(&pkg_path);
                        arg_str.push("\" /S");
                        // if target == InstallTarget::User {
                        //     arg_str.push(" /CurrentUser")
                        // }
                    }
                    _ => {}
                };
                sys::args(&arg_str.as_os_str()).collect()
            }
            _ => sys::args(&OsString::from(pkg_path)).collect(),
        };
        log::debug!("{:?}", &args);
        let prog = args[0].clone();
        args.remove(0);

        // log::debug!("Cmd line: {:?} {:?}", &pkg_path, &args);

        let res = Command::new(&prog).args(&args).output();

        let output = match res {
            Ok(v) => v,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(InstallError::InstallerFailure {
                    source: ProcessError::Io { source: e },
                });
            }
        };

        if !output.status.success() {
            log::error!("{:?}", output);
            return Err(InstallError::InstallerFailure {
                source: ProcessError::Unknown { output },
            });
        }

        Ok(self
            .status_impl(&installer, key, &package, target.clone())
            .unwrap())
    }

    fn uninstall(
        &self,
        key: &PackageKey,
        target: &Self::Target,
    ) -> Result<PackageStatus, UninstallError> {
        let package = match self.resolve_package(key) {
            Some(v) => v,
            None => {
                return Err(UninstallError::NoPackage);
            }
        };

        let installer = match package.installer() {
            None => return Err(UninstallError::NoInstaller),
            Some(v) => v,
        };

        let installer = match installer {
            &Installer::Windows(ref v) => v,
            _ => return Err(UninstallError::WrongInstallerType),
        };

        let regkey = match uninstall_regkey(&installer) {
            Some(v) => v,
            None => return Err(UninstallError::NotInstalled),
        };

        let uninst_string: String = match regkey
            .get_value(Keys::QuietUninstallString)
            .or_else(|_| regkey.get_value(Keys::QuietUninstallString))
        {
            Ok(v) => v,
            Err(_) => {
                return Err(UninstallError::PlatformFailure {
                    message: "No compatible uninstallation method found.",
                })
            }
        };

        let mut raw_args: Vec<OsString> = sys::args(&uninst_string).map(|x| x.clone()).collect();
        let prog = raw_args[0].clone();
        raw_args.remove(0);

        let args: Vec<OsString> = match (&installer.installer_type, &installer.uninstall_args) {
            (_, &Some(ref v)) => sys::args(&v).map(|x| x.clone()).collect(),
            (&Some(ref type_), &None) => {
                let arg_str = match type_.as_ref() {
                    "inno" => "/VERYSILENT /SP- /SUPPRESSMSGBOXES /NORESTART".to_owned(),
                    "msi" => format!("/x \"{}\" /qn /norestart", &installer.product_code),
                    "nsis" => "/S".to_owned(),
                    _ => {
                        return Err(UninstallError::PlatformFailure {
                            message: "Invalid type specified for package installer.",
                        })
                    }
                };
                sys::args(&arg_str).collect()
            }
            _ => {
                return Err(UninstallError::PlatformFailure {
                    message: "Invalid type specified for package installer.",
                })
            }
        };

        let res = Command::new(&prog).args(&args).output();

        let output = match res {
            Ok(v) => v,
            Err(source) => {
                return Err(UninstallError::ProcessFailed {
                    source: ProcessError::Io { source },
                });
            }
        };

        if !output.status.success() {
            return Err(UninstallError::ProcessFailed {
                source: ProcessError::Unknown { output },
            });
        }

        Ok(self
            .status_impl(installer, key, &package, target.clone())
            .unwrap())
    }

    fn status(
        &self,
        key: &PackageKey,
        target: &InstallTarget,
    ) -> Result<PackageStatus, PackageStatusError> {
        log::debug!("status: {}, target: {:?}", &key.to_string(), target);

        let package = match self.resolve_package(key) {
            Some(v) => v,
            None => {
                return Err(PackageStatusError::NoPackage);
            }
        };

        let installer = match package.installer() {
            None => return Err(PackageStatusError::NoInstaller),
            Some(v) => v,
        };

        let installer = match installer {
            &Installer::Windows(ref v) => v,
            _ => return Err(PackageStatusError::WrongInstallerType),
        };

        self.status_impl(installer, key, &package, target.clone())
    }

    fn resolve_package(&self, key: &PackageKey) -> Option<Package> {
        crate::repo::resolve_package(key, &self.repos)
    }

    fn find_package_by_id(&self, package_id: &str) -> Option<(PackageKey, Package)> {
        crate::repo::find_package_by_id(self, package_id, &self.repos)
    }

    // fn find_package_dependencies(
    //     &self,
    //     key: &PackageKey,
    //     // package: &Package,
    //     target: &Self::Target,
    // ) -> Result<Vec<???>, PackageDependencyError> {
    //     unimplemented!()
    //     // let mut resolved = Vec::<String>::new();
    //     // Ok(self.find_package_dependencies_impl(key, package, target.clone(), 0, &mut resolved)?)
    // }

    fn refresh_repos(&self) {
        let config = self.config.read().unwrap();
        *self.repos.write().unwrap() = crate::repo::refresh_repos(&self.config.read().unwrap());
    }

    fn clear_cache(&self) {
        crate::repo::clear_cache(&self.config.read().unwrap())
    }

    fn add_repo(&self, url: String, channel: String) -> Result<bool, Box<dyn std::error::Error>> {
        &self.config.read().unwrap().add_repo(RepoRecord {
            url: Url::parse(&url).unwrap(),
            channel,
        })?;
        self.refresh_repos();
        Ok(true)
    }

    fn remove_repo(
        &self,
        url: String,
        channel: String,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        self.config.read().unwrap().remove_repo(RepoRecord {
            url: Url::parse(&url).unwrap(),
            channel,
        })?;
        self.refresh_repos();
        Ok(true)
    }

    fn update_repo(
        &self,
        index: usize,
        url: String,
        channel: String,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        self.config.read().unwrap().update_repo(
            index,
            RepoRecord {
                url: Url::parse(&url).unwrap(),
                channel,
            },
        )?;
        self.refresh_repos();
        Ok(true)
    }
}

impl std::default::Default for WindowsPackageStore {
    fn default() -> Self {
        let config = StoreConfig::load_or_default(true);
        WindowsPackageStore::new(config)
    }
}

impl WindowsPackageStore {
    pub fn new(config: StoreConfig) -> WindowsPackageStore {
        let store = WindowsPackageStore {
            repos: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(RwLock::new(config)),
        };

        store.refresh_repos();

        store
    }

    pub fn config(&self) -> SharedStoreConfig {
        Arc::clone(&self.config)
    }

    // fn find_package_dependencies_impl(
    //     &self,
    //     key: &PackageKey,
    //     package: &Package,
    //     target: InstallTarget,
    //     level: u8,
    //     resolved: &mut Vec<String>,
    // ) -> Result<Vec<???>, PackageDependencyError> {
    //     unimplemented!()
    // }

    // TODO: this is a sneaky FFI hack, booooo
    // pub fn package_path(&self, key: &PackageKey) -> Option<PathBuf> {
    //     let package = match self.resolve_package(key) {
    //         Some(v) => v,
    //         None => {
    //             return None;
    //         }
    //     };

    //     let installer = match package.installer() {
    //         None => return None,
    //         Some(v) => v,
    //     };

    //     let installer = match *installer {
    //         Installer::Windows(ref v) => v,
    //         _ => return None,
    //     };

    //     let url = match url::Url::parse(&installer.url) {
    //         Ok(v) => v,
    //         Err(_) => return None,
    //     };
    //     let filename = url.path_segments().unwrap().last().unwrap();
    //     let pkg_path = crate::repo::download_path(&self.config(), &url.as_str()).join(filename);

    //     if !pkg_path.exists() {
    //         return None;
    //     }

    //     Some(pkg_path)
    // }

    fn status_impl(
        &self,
        installer: &WindowsInstaller,
        id: &PackageKey,
        package: &Package,
        target: InstallTarget,
    ) -> Result<PackageStatus, PackageStatusError> {
        let inst_key = match uninstall_regkey(&installer) {
            Some(v) => v,
            None => return Ok(PackageStatus::NotInstalled),
        };

        let disp_version: String = match inst_key.get_value(Keys::DisplayVersion) {
            Err(_) => return Err(PackageStatusError::ParsingVersion),
            Ok(v) => v,
        };

        let config = self.config.read().unwrap();

        let skipped_package = config.skipped_package(id);
        let skipped_package = skipped_package.as_ref().map(String::as_ref);

        let status = self::cmp::cmp(&disp_version, &package.version, skipped_package);

        log::debug!("Status: {:?}", &status);
        status
        // .or_else(|_| self::cmp::assembly_cmp(...))
    }
}
