fn main() {
    println!("cargo:rustc-link-search=native=/opt/homebrew/opt/nanomsg/lib");
    println!("cargo:rustc-link-lib=dylib=nanomsg");
}
