use std::io::Result;

fn main() -> Result<()> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/nexus.proto"], &["proto"])?;
    Ok(())
}
