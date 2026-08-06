{
  description = "jj-mesh: peer-to-peer synchronization of jj repositories";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs =
    { self, nixpkgs, ... }:
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
          pkgs = nixpkgs.legacyPackages.${system};

          jj-mesh = pkgs.rustPlatform.buildRustPackage {
            pname = "jj-mesh";
            version = (nixpkgs.lib.importTOML ./Cargo.toml).package.version;

            src = self;
            cargoHash = "sha256-jtQ4A41YIxa9e0axdqEAySfrcTd5j6bzJSvFxcBcabE=";

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
    };
}
