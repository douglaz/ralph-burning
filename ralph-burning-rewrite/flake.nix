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

          # Tests generate mock backend scripts at runtime with
          # #!/usr/bin/env bash shebangs. The sandbox doesn't provide
          # /usr/bin/env, so patch the shebang string in the test source
          # to use the Nix store bash path directly.
          postPatch = ''
            for f in src/contexts/conformance_spec/scenarios.rs tests/unit/tmux_adapter_test.rs; do
              if [ -f "$f" ]; then
                substituteInPlace "$f" \
                  --replace-quiet '#!/usr/bin/env bash' '#!${pkgs.bash}/bin/bash'
              fi
            done
          '';
        };

        staticPackage = pkgs.pkgsStatic.rustPlatform.buildRustPackage (commonArgs // {
          cargoTestFlags = [ "--features" "test-stub" ];
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

        dynamicPackage = pkgs.rustPlatform.buildRustPackage (commonArgs // {
          cargoTestFlags = [ "--features" "test-stub" ];
        });
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
          packages = [
            self.packages.${system}.default
          ] ++ (with pkgs; [
            cargo
            rustc
            rustfmt
            clippy
            rust-analyzer
            git
            gh
            jq
          ]);

          RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
        };
      });
}
