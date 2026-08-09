//! Pulls objects out of a single S3 bucket folder onto local disk.
//!
//! `download_files` fans a batch across tokio tasks rather than awaiting each
//! object in turn, since the work is network-bound. A `Semaphore` caps how many
//! run at once so a large batch can't exhaust connections, and `FileDownloader`
//! is `Clone` so each task gets an owned copy — the `Client` behind it is an
//! `Arc`, so cloning costs a refcount bump rather than a new connection pool.
//!
//! A failure ends the batch: the error propagates and the downloads still in
//! flight are aborted. Because an abort can stop a task between any two writes,
//! nothing is written at its final path — each object streams to a `.part`
//! sibling and is renamed into place only once whole. A file at its real name is
//! therefore always complete, which is what lets a later run skip it. A download
//! that fails cleans up after itself, but an aborted task gets no chance to run
//! async cleanup, so an interrupted batch leaves `.part` debris behind. Retrying
//! the same file truncates its leftover; anything not retried stays until
//! something else removes it.

use aws_config::SdkConfig;
use aws_sdk_s3::Client;
use log::{error, warn};
use std::{
  io::ErrorKind,
  path::{Path, PathBuf},
  sync::Arc,
};
use tokio::{
  fs::{self, File},
  io::AsyncWriteExt,
  sync::Semaphore,
  task::{JoinError, JoinSet},
};

use crate::errors::OrchestraError;

type FinishedDownload = Result<(String, Result<PathBuf, OrchestraError>), JoinError>;

#[derive(Clone)]
pub(crate) struct FileDownloader {
  bucket_name: String,
  client: Client,
  folder: String,
}

impl FileDownloader {
  pub fn new(bucket_name: &str, config: SdkConfig, folder: &str) -> Self {
    Self {
      bucket_name: bucket_name.into(),
      client: Client::new(&config),
      folder: folder.into(),
    }
  }

  fn object_key(&self, filename: &str) -> String {
    format!("{folder}/{filename}", folder = self.folder)
  }

  /// Downloads a single object, returning where it landed, creating the
  /// destination directory if it is missing. The body streams to a sibling
  /// `.part` file that is renamed into place only once it is whole, so the
  /// returned path never names a half-written file.
  pub async fn download_file(&self, output_folder: PathBuf, filename: &str) -> Result<PathBuf, OrchestraError> {
    let destination = output_folder.join(filename);
    let in_progress = partial_path(&destination);

    // Not `output_folder` itself: a filename carrying a prefix, as S3 keys tend
    // to, nests the destination deeper than the folder it was handed.
    if let Some(parent) = destination.parent() {
      fs::create_dir_all(parent).await?;
    }

    match self.stream_object(&in_progress, filename).await {
      Ok(()) => {
        fs::rename(&in_progress, &destination).await?;

        Ok(destination)
      }
      Err(error) => {
        discard(&in_progress).await;

        Err(error)
      }
    }
  }

  async fn stream_object(&self, in_progress: &Path, filename: &str) -> Result<(), OrchestraError> {
    let object_key = self.object_key(filename);
    let mut response = self
      .client
      .get_object()
      .bucket(&self.bucket_name)
      .key(&object_key)
      .send()
      .await?;
    let mut file = File::create(in_progress).await?;

    while let Some(bytes) = response.body.try_next().await? {
      file.write_all(&bytes).await?;
    }

    // `write_all` only fills tokio's buffer; without this the rename can publish
    // a file whose tail never reached the filesystem.
    file.sync_all().await?;

    Ok(())
  }

  /// Downloads every file into `output_folder`, at most `max_concurrent` at a
  /// time. The first failure returns, aborting the downloads still running, so
  /// an error means some files were written and others never started. Paths
  /// come back in completion order, not the order of `filenames`.
  pub async fn download_files(
    &self,
    output_folder: PathBuf,
    filenames: &[String],
    max_concurrent: usize,
  ) -> Result<Vec<PathBuf>, OrchestraError> {
    let permits = Arc::new(Semaphore::new(max_concurrent.max(1)));
    let mut downloads = JoinSet::new();
    let mut downloaded = Vec::with_capacity(filenames.len());

    for filename in filenames {
      let permit = Arc::clone(&permits)
        .acquire_owned()
        .await
        .expect("semaphore stays open for the whole batch");
      let downloader = self.clone();
      let output_folder = output_folder.clone();
      let filename = filename.clone();

      downloads.spawn(async move {
        let _permit = permit;
        let result = downloader.download_file(output_folder, &filename).await;

        (filename, result)
      });

      while let Some(finished) = downloads.try_join_next() {
        downloaded.push(downloaded_path(finished)?);
      }
    }

    while let Some(finished) = downloads.join_next().await {
      downloaded.push(downloaded_path(finished)?);
    }

    Ok(downloaded)
  }
}

/// Appends to the file name rather than replacing its extension, which
/// `with_extension` would do — collapsing `report.csv` and `report.json` onto a
/// single `report.part` that two downloads would then fight over.
fn partial_path(destination: &Path) -> PathBuf {
  let mut partial = destination.to_path_buf().into_os_string();
  partial.push(".part");

  PathBuf::from(partial)
}

/// Best-effort cleanup. The download error that got us here is the one worth
/// returning, so a failed unlink is only logged. A missing file is not a
/// failure: the download may have died before it was created.
async fn discard(in_progress: &Path) {
  match fs::remove_file(in_progress).await {
    Ok(()) => {}
    Err(error) if error.kind() == ErrorKind::NotFound => {}
    Err(error) => warn!("Could not remove {path}: {error}", path = in_progress.display()),
  }
}

/// Unwraps a finished download, naming the file in the log on the way out —
/// `OrchestraError` carries the cause but not which object produced it. A
/// `JoinError` means the task panicked rather than the download failing.
fn downloaded_path(finished: FinishedDownload) -> Result<PathBuf, OrchestraError> {
  match finished {
    Ok((_, Ok(path))) => Ok(path),
    Ok((filename, Err(error))) => {
      error!("Download failed for {filename}: {error}");

      Err(error)
    }
    Err(error) => Err(error.into()),
  }
}
