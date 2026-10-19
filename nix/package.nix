{
  lib,
  pkgs,
  craneLib,
  pkg-config,
  openssl,
  makeBinaryWrapper,
  src,
}:
let
  rustSrc = lib.cleanSourceWith {
    inherit src;
    name = "source";
    filter =
      path: _:
      let
        relative = lib.removePrefix "${src}/" path;
        top = builtins.head (lib.splitString "/" relative);
      in
      builtins.elem top [
        "Cargo.toml"
        "Cargo.lock"
        ".cargo"
        ".sqlx"
        "crates"
        "daemon"
      ]
      || relative == "docs"
      || relative == "docs/example-config.toml";
  };
  frontend = pkgs.callPackage ./frontend.nix {
    src = lib.cleanSourceWith {
      src = src + "/admin-ui";
      name = "source";
      filter = _: _: true;
    };
  };
  args = {
    pname = "arctern";
    version = (builtins.fromTOML (builtins.readFile "${src}/Cargo.toml")).workspace.package.version;
    src = rustSrc;
    strictDeps = true;
    cargoExtraArgs = "--locked --workspace";
    nativeBuildInputs = [ pkg-config ];
    buildInputs = [ openssl ];
    env.SQLX_OFFLINE = "true";
  };
  cargoArtifacts = craneLib.buildDepsOnly args;
in
craneLib.buildPackage (
  args
  // {
    inherit cargoArtifacts;
    nativeBuildInputs = args.nativeBuildInputs ++ [ makeBinaryWrapper ];
    preBuild = "mkdir -p admin-ui; cp -a ${frontend} admin-ui/dist";
    postInstall = ''
      wrapProgram $out/bin/arctern --prefix PATH : '${
        lib.makeBinPath [
          pkgs.openssh
          pkgs.coreutils
          pkgs.util-linux
        ]
      }:/run/booted-system/sw/bin:/run/current-system/sw/bin'
    '';
    passthru = { inherit frontend cargoArtifacts; };
    meta = {
      description = "ZFS replication daemon with an embedded administration UI";
      homepage = "https://github.com/okhsunrog/arctern";
      mainProgram = "arctern";
      license = lib.licenses.mit;
      platforms = [ "x86_64-linux" ];
    };
  }
)
