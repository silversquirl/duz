{
  inputs = {
    nixpkgs.url = "nixpkgs";
    zig.url = "github:silversquirl/zig-flake/compat";

    zig.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    nixpkgs,
    zig,
    ...
  }: let
    forAllSystems = f: builtins.mapAttrs f nixpkgs.legacyPackages;
  in {
    devShells = forAllSystems (system: pkgs: {
      default = pkgs.mkShellNoCC {
        packages = [
          pkgs.bash
          zig.packages.${system}.zig_0_14_1
          pkgs.zls
        ];
      };
    });

    packages = forAllSystems (system: pkgs: {
      default = zig.packages.${system}.zig_0_14_1.makePackage {
        pname = "duz";
        version = "0.0.0";
        src = ./.;
        zigReleaseMode = "fast";
        depsHash = "sha256-FJwawK3v9f1wfF3VBOtlRTFmpBjn3qpWO+uynesSaqQ=";
      };
    });
  };
}
