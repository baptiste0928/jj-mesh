{
  description = "jj-mesh: peer-to-peer synchronization of jj repositories";

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
      self,
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

      homeModules = rec {
        jj-mesh = import ./home-module.nix self;
        default = jj-mesh;
      };

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

          commonArgs = {
            src = craneLib.cleanCargoSource ./.;
            strictDeps = true;
            doCheck = false; # Don't run tests on the flake
          };

          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          jj-mesh = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              JJ_MESH_COMMIT = self.shortRev or self.dirtyShortRev or "unknown";
              nativeBuildInputs = [ pkgs.installShellFiles ];
              postInstall = ''
                installShellCompletion --cmd jj-mesh \
                  --bash <(COMPLETE=bash $out/bin/jj-mesh) \
                  --fish <(COMPLETE=fish $out/bin/jj-mesh) \
                  --zsh <(COMPLETE=zsh $out/bin/jj-mesh)
              '';
              meta = {
                description = "Peer-to-peer synchronization of jj repositories";
                license = nixpkgs.lib.licenses.isc;
                mainProgram = "jj-mesh";
              };
            }
          );
        in
        {
          default = jj-mesh;
          inherit jj-mesh;
        }
      );
    };
}
