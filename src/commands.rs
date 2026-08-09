use clap::{Arg, ArgAction, Command};

fn config_command() -> Command {
  Command::new("config")
    .about("Manage configuration settings")
    .subcommand(
      Command::new("init")
        .about("Initialize data encryption keys for environment")
        .arg(
          Arg::new("folder")
            .short('f')
            .long("folder")
            .value_name("STRING")
            .action(ArgAction::Set)
            .help("Folder name for generated keys")
            .default_value(".keys"),
        )
        .arg(
          Arg::new("filenames")
            .short('n')
            .long("filenames")
            .value_name("STRING")
            .action(ArgAction::Set)
            .help("Comma separated names of files to be downloaded")
            .default_value("public_key.pem,private_key.pem"),
        ),
    )
    .subcommand_required(true)
}

pub fn build_cli() -> Command {
  Command::new("orchestra")
    .about("A set of tools for managing credentials and configuration")
    .subcommand(config_command())
    .subcommand_required(true)
}
