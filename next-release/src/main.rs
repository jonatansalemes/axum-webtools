use serde::Deserialize;
use std::path::Path;
use toml_edit::DocumentMut;

#[derive(Deserialize)]
struct BinCargoPackage {
    version: String,
}

#[derive(Deserialize)]
struct BinCargo {
    package: BinCargoPackage,
}

fn parse<T>(path: &Path) -> (T, String)
where
    T: for<'de> Deserialize<'de>,
{
    let contents = std::fs::read_to_string(path).unwrap();
    let t: T = toml::from_str(&contents).unwrap();
    (t, contents)
}

fn main() {
    let project_folder = std::env::args().nth(2).expect("project folder is required");
    let other_deps = std::env::args().nth(3);
    let cargo_file = Path::new(&project_folder).join("Cargo.toml");
    let (bin_cargo, cargo_file_contents) = parse::<BinCargo>(&cargo_file);
    let current_version = bin_cargo
        .package
        .version
        .parse::<semver::Version>()
        .unwrap();
    let next_version = semver::Version {
        major: current_version.major,
        minor: current_version.minor,
        patch: current_version.patch + 1,
        pre: Default::default(),
        build: Default::default(),
    };
    let mut doc = cargo_file_contents
        .parse::<DocumentMut>()
        .expect("invalid doc");
    doc["package"]["version"] = toml_edit::value(next_version.to_string().to_string());
    std::fs::write(&cargo_file, doc.to_string()).expect("failed to write file");
    if let Some(dep) = other_deps {
        doc["dependencies"][dep] = toml_edit::value(next_version.to_string().to_string());
        std::fs::write(&cargo_file, doc.to_string()).expect("failed to write file");
    }
}
