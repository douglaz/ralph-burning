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
            pkgs.procps
            pkgs.util-linux
            pkgs.rustfmt
            pkgs.clippy
          ];

          # Tests generate mock backend scripts at runtime with
          # #!/usr/bin/env bash shebangs. The sandbox doesn't provide
          # /usr/bin/env, so patch the shebang string in the test source
          # to use the Nix store bash path directly.
          postPatch = ''
            for f in src/contexts/conformance_spec/scenarios.rs tests/unit/tmux_adapter_test.rs tests/run_attach_tmux.rs; do
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

        staticRunPackage = pkgs.pkgsStatic.rustPlatform.buildRustPackage (commonArgs // {
          doCheck = false;
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

        dynamicRunPackage = pkgs.rustPlatform.buildRustPackage (commonArgs // {
          doCheck = false;
        });
      in
      {
        packages =
          {
            default = if isLinux then staticPackage else dynamicPackage;
            dynamic = dynamicPackage;
            run = if isLinux then staticRunPackage else dynamicRunPackage;
            dynamic-run = dynamicRunPackage;
          }
          // pkgs.lib.optionalAttrs isLinux {
            static = staticPackage;
            static-run = staticRunPackage;
          };

        apps.default = flake-utils.lib.mkApp {
          drv = self.packages.${system}.run;
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
            procps
            util-linux
          ];

          RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";

          shellHook = ''
            if [ -d .git ] && [ -d .githooks ]; then
              current_hooks_path=$(git config core.hooksPath || echo "")
              if [ "$current_hooks_path" != ".githooks" ]; then
                git config core.hooksPath .githooks
                echo "Git hooks configured (.githooks/pre-commit, .githooks/pre-push)"
                echo "To disable: git config --unset core.hooksPath"
              fi
            fi
          '';
        };
      });
}
