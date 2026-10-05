# Home Manager module running the jj-mesh daemon as a user service

self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.jj-mesh;
  tomlFormat = pkgs.formats.toml { };

  environment = {
    RUST_LOG = "jj_mesh=info";
    JJ_BIN = lib.getExe cfg.jjPackage;
  };
in
{
  options.services.jj-mesh = {
    enable = lib.mkEnableOption "jj-mesh, a bi-directional sync daemon for jj repositories";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalMD "the package built by the jj-mesh flake";
      description = "The jj-mesh package providing the daemon and CLI.";
    };

    jjPackage = lib.mkOption {
      type = lib.types.package;
      default =
        let
          jj = config.programs.jujutsu;
        in
        if jj.enable && jj.package != null then jj.package else pkgs.jujutsu;
      defaultText = lib.literalExpression "config.programs.jujutsu.package or pkgs.jujutsu";
      description = ''
        The jj binary the daemon invokes (sets `JJ_BIN`). Defaults to the
        Home Manager managed jj when {option}`programs.jujutsu` is enabled,
        so the daemon runs the same jj as the user.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    home.packages = [ cfg.package ];

    # Record that the service is managed by home-manager
    xdg.configFile."jj-mesh/service.toml".source = tomlFormat.generate "jj-mesh-service.toml" {
      installer = "home-manager";
      label = if pkgs.stdenv.isDarwin then "org.nix-community.home.jj-mesh" else "jj-mesh";
    };

    systemd.user.services.jj-mesh = lib.mkIf pkgs.stdenv.isLinux {
      Unit = {
        Description = "jj-mesh sync daemon";
        After = [ "network.target" ];
      };
      Service = {
        ExecStart = "${lib.getExe cfg.package} run-daemon";
        Restart = "on-failure";
        RestartSec = 5;
        Environment = lib.mapAttrsToList (name: value: "${name}=${value}") environment;
      };
      Install.WantedBy = [ "default.target" ];
    };

    launchd.agents.jj-mesh = lib.mkIf pkgs.stdenv.isDarwin {
      enable = true;
      config = {
        ProgramArguments = [
          (lib.getExe cfg.package)
          "run-daemon"
        ];
        KeepAlive.SuccessfulExit = false;
        RunAtLoad = true;
        ProcessType = "Background";
        StandardOutPath = "${config.home.homeDirectory}/Library/Logs/jj-mesh.log";
        StandardErrorPath = "${config.home.homeDirectory}/Library/Logs/jj-mesh.log";
        EnvironmentVariables = environment;
      };
    };
  };
}
