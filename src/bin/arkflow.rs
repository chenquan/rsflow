//! 流处理引擎启动器Stream processing engine launcher
//!
//! This module provides a command-line tool for starting a stream processing engine based on a configuration file.
//! Support loading configurations from YAML, JSON, or TOML format files.

use std::process;

use clap::{Arg, Command};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

use arkflow::config::EngineConfig;
use arkflow::{input, output};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    input::init();
    output::init();

    let matches = Command::new("arkflow")
        .version("0.1.0")
        .author("chenquan")
        .about("High-performance Rust stream processing engine, providing powerful data stream processing capabilities, supporting multiple input/output sources and processors.")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Specify the profile path.")
                .required(true),
        )
        .arg(
            Arg::new("validate")
                .short('v')
                .long("validate")
                .help("Only the profile is verified, not the engine is started.")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    // Get the profile path
    let config_path = matches.get_one::<String>("config").unwrap();

    // Get the profile path
    let config = match EngineConfig::from_file(config_path) {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to load configuration file: {}", e);
            process::exit(1);
        }
    };

    // If you just verify the configuration, exit it
    if matches.get_flag("validate") {
        info!("The config is validated.");
        return Ok(());
    }

    // Initialize the logging system
    init_logging(&config);

    // Create and run all flows
    let mut streams = Vec::new();
    let mut handles = Vec::new();

    for (i, stream_config) in config.streams.iter().enumerate() {
        info!("Initializing flow #{}", i + 1);

        match stream_config.build() {
            Ok(stream) => {
                streams.push(stream);
            }
            Err(e) => {
                error!("Initializing flow #{} error: {}", i + 1, e);
                process::exit(1);
            }
        }
    }

    for (i, mut stream) in streams.into_iter().enumerate() {
        info!("Starting flow #{}", i + 1);

        let handle = tokio::spawn(async move {
            match stream.run().await {
                Ok(_) => info!("Flow #{} completed successfully", i + 1),
                Err(e) => {
                    error!("Stream #{} ran with error: {}", i + 1, e)
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all flows to complete
    for handle in handles {
        handle.await?;
    }

    info!("All flow tasks have been complete");
    Ok(())
}

/// Initialize the logging system
fn init_logging(config: &EngineConfig) -> () {
    let log_level = if let Some(logging) = &config.logging {
        match logging.level.as_str() {
            "trace" => Level::TRACE,
            "debug" => Level::DEBUG,
            "info" => Level::INFO,
            "warn" => Level::WARN,
            "error" => Level::ERROR,
            _ => Level::INFO,
        }
    } else {
        Level::INFO
    };

    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("You can't set a global default log subscriber");
}
