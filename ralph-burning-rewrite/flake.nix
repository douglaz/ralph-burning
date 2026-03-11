{
  description = "Ralph Burning — AI delivery orchestrator rewrite";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        isLinux = pkgs.stdenv.hostPlatform.isLinux;

        commonArgs = {
          pname = "ralph-burning";
          version = "0.1.0";
          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          nativeBuildInputs = [
            pkgs.git
          ];

          nativeCheckInputs = [
            pkgs.bash
            pkgs.rustfmt
            pkgs.clippy
          ];
        };

        staticPackage = pkgs.pkgsStatic.rustPlatform.buildRustPackage (commonArgs // {
          postInstall = ''
            echo "verifying static linkage..."
            file_output="$(${pkgs.file}/bin/file "$out/bin/ralph-burning")"
            echo "$file_output"
            if ! echo "$file_output" | grep -Eq "statically linked|static-pie linked"; then
              echo "FAIL: binary is NOT statically linked"
              exit 1
            fi
          '';
        });

        dynamicPackage = pkgs.rustPlatform.buildRustPackage commonArgs;
      in
      {
        packages =
          {
            default = if isLinux then staticPackage else dynamicPackage;
            dynamic = dynamicPackage;
          }
          // pkgs.lib.optionalAttrs isLinux {
            static = staticPackage;
          };

        apps.default = flake-utils.lib.mkApp {
          drv = self.packages.${system}.default;
        };

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            cargo
            rustc
            rustfmt
            clippy
            rust-analyzer
            git
            gh
            jq
          ];

          RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
        };
      });
}
