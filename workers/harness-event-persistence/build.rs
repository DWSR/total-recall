use std::{env, fs, io, path::PathBuf};

fn main() -> io::Result<()> {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let repository_root = manifest_dir.join("../..");
    let proto_root = repository_root.join("proto");
    let proto_file = proto_root.join("total_recall/harness/v1/events.proto");
    let descriptor_path = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("events.fds");

    println!("cargo:rerun-if-changed={}", proto_file.display());

    prost_build::Config::new()
        .file_descriptor_set_path(&descriptor_path)
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile_protos(&[&proto_file], &[&proto_root])?;

    let descriptor_set = fs::read(&descriptor_path)?;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)
        .map_err(io::Error::other)?
        .extern_path(".google.protobuf", "::pbjson_types")
        .emit_fields()
        .preserve_proto_field_names()
        .build(&[".total_recall.harness.v1"])
        .map_err(io::Error::other)
}
