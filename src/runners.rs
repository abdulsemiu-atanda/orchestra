use aws_config::BehaviorVersion;
use clap::ArgMatches;
use dialoguer::{Input, theme::ColorfulTheme};
use log::info;
use std::path::PathBuf;

use crate::services::file_downloader::FileDownloader;

pub async fn run_config_command(matches: &ArgMatches) -> std::io::Result<()> {
  match matches
    .subcommand()
    .expect("Clap should prevent calling this without a subcommand")
  {
    ("init", flags) => {
      let output_folder = flags
        .get_one::<String>("folder")
        .map(|value| value.to_owned())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".keys"));
      let client_id: String = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Credentials Client ID")
        .interact_text()
        .expect("Failed to process client name input");
      let filenames = vec!["publick_key.pem".to_owned(), "private_key.pem".to_owned()];
      let bucket_name = dotenvy::var("AWS_S3_BUCKET_NAME").expect("AWS_S3_BUCKET_NAME must be set in .env");
      let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
      let downloader = FileDownloader::new(&bucket_name, config, &client_id);

      info!("Downloading data encryption keys for environment");
      downloader
        .download_files(output_folder, &filenames, 10)
        .await
        .expect("Failed to download data encryption keys");
      info!("Data encryption keys initialized successfully");
    }
    _ => unreachable!("Clap should prevent getting here"),
  };

  Ok(())
}
