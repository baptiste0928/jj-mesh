{
  description = "jj-mesh: peer-to-peer synchronization of jj repositories";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
      ...
    }:
    let
      forEachSystem = nixpkgs.lib.genAttrs [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      pkgsFor =
        system:
        import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };

      # Toolchain pinned by rust-toolchain.toml
      toolchainFor = pkgs: pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
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
          pkgs = pkgsFor system;
          toolchain = toolchainFor pkgs;
          rustPlatform = pkgs.makeRustPlatform {
            cargo = toolchain;
            rustc = toolchain;
          };

          jj-mesh = rustPlatform.buildRustPackage {
            pname = "jj-mesh";
            version = (nixpkgs.lib.importTOML ./Cargo.toml).package.version;

            src = self;
            cargoHash = "sha256-96Lv+GsFvsCXK2uh0iyMksiV4/8fFUdMC1CWwqDogOQ=";

            doCheck = false; # Don't run tests on the flake
            env.JJ_MESH_COMMIT = self.shortRev or self.dirtyShortRev or "unknown";

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
          };
        in
        {
          default = jj-mesh;
          inherit jj-mesh;
        }
      );

      devShells = forEachSystem (
        system:
        let
          pkgs = pkgsFor system;
          toolchain = (toolchainFor pkgs).override {
            extensions = [
              "rust-src"
              "rust-analyzer"
            ];
          };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              rustup
              pinact # pin github actions versions
            ];
            env.RUSTUP_TOOLCHAIN = "${toolchain}";
          };
        }
      );
    };
}
