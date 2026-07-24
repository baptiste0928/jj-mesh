{
  description = "jj-mesh: a bi-directional sync service for jj repositories";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      nixpkgs,
      crane,
      rust-overlay,
      ...
    }:
    let
      forEachSystem = nixpkgs.lib.genAttrs [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
    in
    {
      formatter = forEachSystem (system: nixpkgs.legacyPackages.${system}.nixfmt-tree);

      packages = forEachSystem (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };

          # Build with the toolchain pinned in rust-toolchain.toml
          craneLib = (crane.mkLib pkgs).overrideToolchain (
            p: p.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml
          );

          jj-mesh = craneLib.buildPackage {
            src = craneLib.cleanCargoSource ./.;
            strictDeps = true;
            doCheck = false; # Don't run tests on the flake
            meta = {
              description = "Bi-directional sync service for jj repositories";
              license = nixpkgs.lib.licenses.isc;
              mainProgram = "jj-mesh";
            };
          };
        in
        {
          default = jj-mesh;
          inherit jj-mesh;
        }
      );
    };
}
