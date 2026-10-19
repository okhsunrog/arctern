{
  description = "Arctern ZFS replication daemon";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/fe13242281a67641c0102cdb1087f0ebe586c7df";
    crane.url = "github:ipetkov/crane";
  };
  outputs =
    {
      self,
      nixpkgs,
      crane,
    }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      mkPackage =
        pkgs:
        pkgs.callPackage ./nix/package.nix {
          craneLib = crane.mkLib pkgs;
          src = self;
        };
    in
    {
      packages.${system} = {
        default = self.packages.${system}.arctern;
        arctern = mkPackage pkgs;
      };
      lib.mkPackage = mkPackage;
      nixosModules.default = import ./nix/module.nix { inherit self; };
      checks.${system} = {
        package = self.packages.${system}.arctern;
        service = import ./nix/test.nix { inherit pkgs self; };
      };
      formatter.${system} = pkgs.nixfmt-tree;
    };
}
