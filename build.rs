fn main() {
    println!("cargo:rerun-if-env-changed=LAZILY_SPEC_DIR");
    if std::env::var_os("CARGO_FEATURE_PROTOBUF").is_none() {
        return;
    }

    let spec_dir = std::env::var_os("LAZILY_SPEC_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("../lazily-spec"));
    let proto = spec_dir.join("proto/lazily/graph_boundary/v1/graph_boundary.proto");
    println!("cargo:rerun-if-changed={}", proto.display());

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc is available");
    // SAFETY: Cargo runs this build script as a single-threaded process, and
    // prost-build reads PROTOC synchronously before the process exits.
    unsafe { std::env::set_var("PROTOC", protoc) };
    prost_build::compile_protos(&[proto], &[spec_dir.join("proto")])
        .expect("canonical graph-boundary proto must compile");
}
