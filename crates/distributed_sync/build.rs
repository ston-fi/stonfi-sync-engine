use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let mut prost_config = prost_build::Config::new();
    prost_config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);

    tonic_prost_build::configure().compile_with_config(prost_config, &["proto/task.proto"], &["proto"])?;
    Ok(())
}
