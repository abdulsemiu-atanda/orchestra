mod commands;
mod runners;
mod services;
mod errors;

use commands::build_cli;
use runners::run_config_command;

#[tokio::main]
async fn main() -> std::io::Result<()> {
  env_logger::init();

  let matches = build_cli().get_matches();

  match matches
    .subcommand()
    .expect("Clap should prevent calling this without a subcommand.")
  {
    ("config", matches) => run_config_command(matches).await?,
    _ => unreachable!("The cli parser should prevent reaching here"),
  };
  Ok(())
}
