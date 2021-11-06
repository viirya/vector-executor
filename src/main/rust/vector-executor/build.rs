//! Build script for generating codes from .proto files.

use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=src/proto/*.proto");

    prost_build::compile_protos(&["src/proto/expr.proto"], &["src/"])?;
    Ok(())
}