{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    zig.url = "github:silversquirl/zig-flake/compat";
    zls.url = "github:zigtools/zls";

    zig.inputs.nixpkgs.follows = "nixpkgs";
    zls.inputs.nixpkgs.follows = "nixpkgs";
    zls.inputs.zig-overlay.follows = "zig";
  };

  outputs = {
    nixpkgs,
    zig,
    zls,
    ...
  }: let
    forAllSystems = f: builtins.mapAttrs f nixpkgs.legacyPackages;
  in {
    devShells = forAllSystems (system: pkgs: {
      default = pkgs.mkShellNoCC {
        packages = [
          pkgs.bash
          zig.packages.${system}.nightly
          (zls.packages.${system}.zls.overrideAttrs {doCheck = false;})
        ];
      };
    });

    packages = forAllSystems (system: pkgs: let
      inherit (pkgs) lib stdenvNoCC;

      zigPkg = zig.packages.${system}.nightly;
      zigTarget = plat:
        builtins.replaceStrings
        ["-darwin" "-unknown"]
        ["-macos" "-none"]
        "${plat.system}-${plat.parsed.abi.name}";
    in {
      default = stdenvNoCC.mkDerivation {
        pname = "duz";
        version = "0.0.0";
        src = ./.;
        nativeBuildInputs = [zigPkg];

        deps = stdenvNoCC.mkDerivation {
          name = "duz-deps";
          src = lib.fileset.toSource {
            root = ./.;
            fileset = lib.fileset.unions [
              ./build.zig
              ./build.zig.zon
            ];
          };
          nativeBuildInputs = [zigPkg];
          buildCommand = ''
            export ZIG_GLOBAL_CACHE_DIR="$PWD/zig-cache"
            export ZIG_LOCAL_CACHE_DIR="$ZIG_GLOBAL_CACHE_DIR"
            cd "$src"
            zig build --color off --fetch
            cp -r "$ZIG_GLOBAL_CACHE_DIR/p" "$out"
          '';

          outputHash = "sha256-FJwawK3v9f1wfF3VBOtlRTFmpBjn3qpWO+uynesSaqQ=";
          outputHashMode = "recursive";
        };

        cpuFeatures = "baseline";
        buildPhase = ''
          export ZIG_GLOBAL_CACHE_DIR="$PWD/zig-cache"
          export ZIG_LOCAL_CACHE_DIR="$ZIG_GLOBAL_CACHE_DIR"

          runHook preBuild

          zig build --color off --prefix "$out" --system "$deps" --release=fast \
            -Dtarget=${zigTarget pkgs.hostPlatform} -Dcpu="$cpuFeatures"

          runHook postBuild
        '';
      };
    });
  };
}
