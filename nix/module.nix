{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.arctern;
in
{
  options.services.arctern = {
    enable = lib.mkEnableOption "Arctern ZFS replication";
    package = lib.mkOption {
      type = lib.types.package;
      default = self.lib.mkPackage pkgs;
    };
    configFile = lib.mkOption {
      type = lib.types.str;
      default = "/etc/arctern/arctern.toml";
      description = "Runtime configuration path. Keep secrets outside the Nix store.";
    };
  };
  config = lib.mkIf cfg.enable {
    environment.systemPackages = [ cfg.package ];
    systemd.services.arctern = {
      description = "Arctern ZFS replication daemon";
      wantedBy = [ "multi-user.target" ];
      after = [
        "zfs.target"
        "network-online.target"
      ];
      wants = [
        "zfs.target"
        "network-online.target"
      ];
      path = [
        config.boot.zfs.package
        pkgs.openssh
        pkgs.coreutils
        pkgs.util-linux
      ];
      serviceConfig = {
        ExecStart = "${cfg.package}/bin/arctern daemon --config ${lib.escapeShellArg cfg.configFile} --socket /run/arctern/arctern.sock";
        Restart = "on-failure";
        RestartSec = "5s";
        TimeoutStopSec = "30s";
        User = "root";
        UMask = "0077";
        StateDirectory = "arctern";
        StateDirectoryMode = "0700";
        RuntimeDirectory = "arctern";
        RuntimeDirectoryMode = "0755";
        NoNewPrivileges = true;
        ProtectSystem = "full";
        PrivateTmp = true;
        ProtectKernelTunables = true;
        ProtectKernelModules = true;
        ProtectControlGroups = true;
        RestrictRealtime = true;
        RestrictSUIDSGID = true;
        LockPersonality = true;
        RestrictAddressFamilies = [
          "AF_UNIX"
          "AF_INET"
          "AF_INET6"
        ];
      };
    };
  };
}
