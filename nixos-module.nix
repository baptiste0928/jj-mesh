# NixOS module running the jj-mesh daemon as a user service

self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.jj-mesh;

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
      default = pkgs.jujutsu;
      defaultText = lib.literalExpression "pkgs.jujutsu";
      description = "The jj binary the daemon invokes (sets `JJ_BIN`).";
    };

    users = lib.mkOption {
      type = lib.types.nonEmptyListOf lib.types.str;
      example = [ "alice" ];
      description = "Users to run the daemon for.";
    };
  };

  config = lib.mkIf cfg.enable {
    environment.systemPackages = [ cfg.package ];

    systemd.user.services.jj-mesh = {
      description = "jj-mesh sync daemon";
      after = [ "network.target" ];
      # Triggering conditions (`|`) are OR-ed, so any listed user matches
      unitConfig.ConditionUser = map (user: "|${user}") cfg.users;
      serviceConfig = {
        Type = "exec";
        ExecStart = "${lib.getExe cfg.package} run-daemon";
        Restart = "on-failure";
        RestartSec = 5;
      };
      inherit environment;
      wantedBy = [ "default.target" ];
    };
  };
}
