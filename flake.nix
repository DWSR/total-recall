{
  description = "Development environment for total-recall";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.nixpkgsDarwin.url = "github:NixOS/nixpkgs/nixpkgs-26.05-darwin";
  inputs.rust-overlay = {
    url = "github:oxalica/rust-overlay";
    flake = false;
  };

  outputs =
    {
      self,
      nixpkgs,
      nixpkgsDarwin,
      rust-overlay,
      ...
    }:
    let
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];
      liveTestSystems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      cargoWorkspaceSource = nixpkgs.lib.fileset.toSource {
        root = ./.;
        fileset = nixpkgs.lib.fileset.unions [
          ./.github/workflows/ci.yml
          ./Cargo.lock
          ./Cargo.toml
          ./clients
          ./crates
          ./integration
          ./proto
          ./workers
        ];
      };
      mkSearchPostgresql =
        postgresql:
        postgresql.withPackages (ps: [
          ps.pg_textsearch
          ps.pgvector
        ]);
      mkExtensionVerification =
        pkgs: postgresql: major:
        pkgs.runCommand "postgresql${toString major}-search-extension-verify"
          {
            nativeBuildInputs = [
              postgresql
              pkgs.coreutils
            ];
          }
          ''
            ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/verify-extensions.sh} \
              ${postgresql}/bin \
              ${toString major} > "$out"
            ${pkgs.coreutils}/bin/cat "$out"
          '';
      mkFixtureVerification =
        pkgs: postgresql: major:
        pkgs.runCommand "postgresql${toString major}-search-fixture-verify"
          {
            nativeBuildInputs = [
              postgresql
              pkgs.coreutils
            ];
          }
          ''
            ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/setup.sh} \
              ${postgresql}/bin \
              ${toString major} \
              -- \
              ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/verify-fixture.sh} > "$out"
            ${pkgs.coreutils}/bin/cat "$out"
          '';
      mkMemoryStorageVerification =
        pkgs: postgresql: major:
        pkgs.runCommand "postgresql${toString major}-memory-storage-verify"
          {
            nativeBuildInputs = [
              postgresql
              pkgs.coreutils
            ];
            MEMORY_SCHEMA_MIGRATION = "${./crates/memory-store/migrations/0001_memory_schema_search.sql}";
          }
          ''
            ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/setup.sh} \
              ${postgresql}/bin \
              ${toString major} \
              -- \
              ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/verify-memory-storage.sh} > "$out"
            ${pkgs.coreutils}/bin/cat "$out"
          '';
      mkMemorySearchHeadsVerification =
        pkgs: postgresql: major:
        pkgs.runCommand "postgresql${toString major}-memory-search-heads-verify"
          {
            nativeBuildInputs = [
              postgresql
              pkgs.coreutils
            ];
            MEMORY_SCHEMA_MIGRATION = "${./crates/memory-store/migrations/0001_memory_schema_search.sql}";
          }
          ''
            ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/setup.sh} \
              ${postgresql}/bin \
              ${toString major} \
              -- \
              ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/verify-memory-search-heads.sh} > "$out"
              ${pkgs.coreutils}/bin/cat "$out"
            '';
      mkMemoryEmbeddingLifecycleVerification =
        pkgs: postgresql: major:
        pkgs.runCommand "postgresql${toString major}-memory-embedding-lifecycle-verify"
          {
            nativeBuildInputs = [
              postgresql
              pkgs.coreutils
            ];
            MEMORY_SCHEMA_MIGRATION = "${./crates/memory-store/migrations/0001_memory_schema_search.sql}";
          }
          ''
            ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/setup.sh} \
              ${postgresql}/bin \
              ${toString major} \
              -- \
              ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/verify-memory-embedding-lifecycle.sh} > "$out"
            ${pkgs.coreutils}/bin/cat "$out"
          '';
      mkMemoryEmbeddingWorkVerification =
        pkgs: postgresql: major:
        let
          filteredSource = cargoWorkspaceSource;
        in
        pkgs.runCommand "postgresql${toString major}-memory-embedding-work-verify"
          {
            nativeBuildInputs = [
              postgresql
              pkgs.coreutils
            ];
            MEMORY_SCHEMA_MIGRATION = "${filteredSource}/crates/memory-store/migrations/0001_memory_schema_search.sql";
          }
          ''
            ${pkgs.bash}/bin/bash \
              ${filteredSource}/integration/memory-postgres-smoke/scripts/setup.sh \
              ${postgresql}/bin \
              ${toString major} \
              -- \
              ${pkgs.bash}/bin/bash \
              ${filteredSource}/integration/memory-postgres-smoke/scripts/verify-memory-embedding-work.sh > "$out"
            ${pkgs.coreutils}/bin/cat "$out"
          '';
      mkMemoryVersionHeadsVerification =
        pkgs: postgresql: major:
        pkgs.runCommand "postgresql${toString major}-memory-version-heads-verify"
          {
            nativeBuildInputs = [
              postgresql
              pkgs.coreutils
            ];
            MEMORY_SCHEMA_MIGRATION = "${./crates/memory-store/migrations/0001_memory_schema_search.sql}";
          }
          ''
            ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/setup.sh} \
              ${postgresql}/bin \
              ${toString major} \
              -- \
              ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/verify-memory-version-heads.sh} > "$out"
            ${pkgs.coreutils}/bin/cat "$out"
          '';
      mkMemoryBm25Verification =
        pkgs: postgresql: major:
        pkgs.runCommand "postgresql${toString major}-memory-bm25-verify"
          {
            nativeBuildInputs = [
              postgresql
              pkgs.coreutils
            ];
            MEMORY_SCHEMA_MIGRATION = "${./crates/memory-store/migrations/0001_memory_schema_search.sql}";
          }
          ''
            ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/setup.sh} \
              ${postgresql}/bin \
              ${toString major} \
              -- \
              ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/verify-memory-bm25.sh} > "$out"
            ${pkgs.coreutils}/bin/cat "$out"
          '';
      mkMemoryVectorVerification =
        pkgs: postgresql: major:
        pkgs.runCommand "postgresql${toString major}-memory-vector-verify"
          {
            nativeBuildInputs = [
              postgresql
              pkgs.coreutils
            ];
            MEMORY_SCHEMA_MIGRATION = "${./crates/memory-store/migrations/0001_memory_schema_search.sql}";
          }
          ''
            ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/setup.sh} \
              ${postgresql}/bin \
              ${toString major} \
              -- \
              ${pkgs.bash}/bin/bash \
              ${./integration/memory-postgres-smoke/scripts/verify-memory-vector.sh} > "$out"
            ${pkgs.coreutils}/bin/cat "$out"
          '';
      mkSessionPostProcessingVerification =
        pkgs: postgresql: major:
        pkgs.runCommand "postgresql${toString major}-session-post-processing-verify"
          {
            nativeBuildInputs = [
              postgresql
              pkgs.coreutils
            ];
            SESSION_POST_PROCESSING_SCHEMA_MIGRATION = "${cargoWorkspaceSource}/workers/session-post-processing/migrations/0001_session_post_processing.sql";
          }
          ''
            {
              ${pkgs.bash}/bin/bash \
                ${cargoWorkspaceSource}/integration/session-post-processing-postgres-smoke/scripts/verify-start-failure-cleanup.sh
              ${pkgs.bash}/bin/bash \
                ${cargoWorkspaceSource}/integration/session-post-processing-postgres-smoke/scripts/setup.sh \
                ${postgresql}/bin \
                ${toString major} \
                -- \
                ${pkgs.bash}/bin/bash \
                ${cargoWorkspaceSource}/integration/session-post-processing-postgres-smoke/scripts/verify.sh
            } > "$out"
            ${pkgs.coreutils}/bin/cat "$out"
          '';
      mkKnowledgeGraphVerification =
        pkgs: graphSource: postgresql: major: rowChangeMode: suite: verifier:
        pkgs.runCommand "postgresql${toString major}-knowledge-graph-${suite}-verify"
          {
            nativeBuildInputs = [
              postgresql
              pkgs.bash
              pkgs.coreutils
            ];
          }
          ''
            ${pkgs.bash}/bin/bash \
              ${graphSource}/integration/knowledge-graph-postgres-smoke/scripts/setup.sh \
              ${postgresql}/bin \
              ${toString major} \
              ${rowChangeMode} \
              -- \
              ${pkgs.bash}/bin/bash \
              ${graphSource}/integration/knowledge-graph-postgres-smoke/scripts/${verifier} > "$out"
            ${pkgs.coreutils}/bin/cat "$out"
          '';
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = import (if system == "x86_64-darwin" then nixpkgsDarwin else nixpkgs) {
            inherit system;
            config.allowDeprecatedx86_64Darwin = "force";
            overlays = nixpkgs.lib.optional (system == "x86_64-darwin") (import rust-overlay);
          };
          rustToolchain =
            if system == "x86_64-darwin" then
              pkgs.rust-bin.stable."1.98.1".minimal.override {
                extensions = [
                  "clippy"
                  "rustfmt"
                  "rust-src"
                ];
              }
            else
              null;
          rustToolchainPackages =
            if system == "x86_64-darwin" then
              [ rustToolchain ]
            else
              with pkgs;
              [
                cargo
                clippy
                rustc
                rustfmt
              ];
          iiiRelease =
            {
              aarch64-darwin = {
                target = "aarch64-apple-darwin";
                hash = "sha256-xCqLwRauaHLb9GIrLuRJEDKgbNK3UxKvuCqPbGR6ChI=";
              };
              aarch64-linux = {
                target = "aarch64-unknown-linux-gnu";
                hash = "sha256-YI8a0WM0jngYpV94ShHi7+KwPYE1MquWokMhHEJ57zA=";
              };
              x86_64-darwin = {
                target = "x86_64-apple-darwin";
                hash = "sha256-pD4rVN/LqWUC629pgPJIx6oWAFXdx08CW1ZBC/AHwyY=";
              };
              x86_64-linux = {
                target = "x86_64-unknown-linux-gnu";
                hash = "sha256-Tvpaj1PfGzCW1M0cJlkLj+pNZlPxpxs7hD/gbwcsmLc=";
              };
            }
            .${system};
          iii = pkgs.stdenvNoCC.mkDerivation {
            pname = "iii";
            version = "0.24.0";

            src = pkgs.fetchurl {
              url = "https://github.com/iii-hq/iii/releases/download/iii%2Fv0.24.0/iii-${iiiRelease.target}.tar.gz";
              inherit (iiiRelease) hash;
            };

            sourceRoot = ".";

            installPhase = ''
              runHook preInstall
              install -Dm755 iii "$out/bin/iii"
              runHook postInstall
            '';
          };
          mkOpenCode =
            {
              pname,
              version,
              sources,
              archiveFormats,
            }:
            let
              archiveFormat =
                archiveFormats.${system} or (throw "Unsupported archive format for ${pname}: ${system}");
              binaryPath = if archiveFormat == "tar-package" then "package/bin/opencode" else "opencode";
            in
            pkgs.stdenvNoCC.mkDerivation {
              inherit pname version;
              src = sources.${system} or (throw "Unsupported system for ${pname}: ${system}");
              strictDeps = true;
              dontUnpack = true;
              dontBuild = true;
              dontStrip = true;

              nativeBuildInputs = [
                pkgs.makeWrapper
              ]
              ++ pkgs.lib.optionals (archiveFormat == "zip") [ pkgs.unzip ]
              ++ pkgs.lib.optionals (archiveFormat != "zip") [ pkgs.gnutar ]
              ++ pkgs.lib.optionals pkgs.stdenvNoCC.hostPlatform.isLinux [
                pkgs.autoPatchelfHook
              ];
              buildInputs = pkgs.lib.optionals pkgs.stdenvNoCC.hostPlatform.isLinux [ pkgs.stdenv.cc.cc.lib ];

              installPhase =
                if archiveFormat == "zip" then
                  ''
                    install -dm755 "$out/bin"
                    unzip -q "$src" -d "$TMPDIR"
                    install -Dm755 "$TMPDIR/opencode" "$out/bin/${pname}"
                  ''
                else
                  ''
                    install -dm755 "$out/bin"
                    tar -xzf "$src" -O ${binaryPath} > "$out/bin/${pname}"
                    chmod 755 "$out/bin/${pname}"
                  '';

              postInstall = ''
                wrapProgram "$out/bin/${pname}" \
                  --set OPENCODE_DISABLE_AUTOUPDATE 1 \
                  --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.ripgrep ]}
              '';

              doInstallCheck = pkgs.stdenvNoCC.buildPlatform.canExecute pkgs.stdenvNoCC.hostPlatform;
              nativeInstallCheckInputs = [
                pkgs.versionCheckHook
                pkgs.writableTmpDirAsHomeHook
              ];
              versionCheckProgramArg = "--version";
              versionCheckKeepEnvironment = [ "HOME" ];

              meta = {
                description = "Pinned OpenCode ${version} test binary";
                homepage = "https://github.com/anomalyco/opencode";
                license = pkgs.lib.licenses.mit;
                mainProgram = pname;
                platforms = liveTestSystems;
              };
            };
          opencodeV1Sources = {
            "aarch64-darwin" = pkgs.fetchurl {
              url = "https://github.com/anomalyco/opencode/releases/download/v1.18.29/opencode-darwin-arm64.zip";
              hash = "sha256-/nZPfzYMWEqD4Y3V8j+xprJyX17ohUsCUv5Vj3eY6UY=";
            };
            "aarch64-linux" = pkgs.fetchurl {
              url = "https://github.com/anomalyco/opencode/releases/download/v1.18.29/opencode-linux-arm64.tar.gz";
              hash = "sha256-cLr3aTlcpOemiSQCZTDDkOrOGU87fkkZ1O/LKqLu08A=";
            };
            "x86_64-linux" = pkgs.fetchurl {
              url = "https://github.com/anomalyco/opencode/releases/download/v1.18.29/opencode-linux-x64-baseline.tar.gz";
              hash = "sha256-A6Py8GPiNHfj5MOnOOs4n1bFpuz1TWpabZHKq1V/BC0=";
            };
          };
          opencodeV2Sources = {
            "aarch64-darwin" = pkgs.fetchurl {
              url = "https://registry.npmjs.org/@opencode/cli-darwin-arm64/-/cli-darwin-arm64-2.0.10.tgz";
              hash = "sha256-8lOTGXL9/uMe+uiks/mHymbMzFIVcMBvXI0g74UH4Y4=";
            };
            "aarch64-linux" = pkgs.fetchurl {
              url = "https://registry.npmjs.org/@opencode/cli-linux-arm64/-/cli-linux-arm64-2.0.10.tgz";
              hash = "sha256-z1QWZ2JARV3FqYUAI3rylssaAO+9LMowKtIskuoIDrw=";
            };
            "x86_64-linux" = pkgs.fetchurl {
              url = "https://registry.npmjs.org/@opencode/cli-linux-x64-baseline/-/cli-linux-x64-baseline-2.0.10.tgz";
              hash = "sha256-cAxND8wgnkL2HBD5dzMx0dH37/NWcJce4xazhkGhp2w=";
            };
          };
          liveToolchainPackages =
            if builtins.elem system liveTestSystems then
              [
                pkgs.bun
                (mkOpenCode {
                  pname = "opencode-v1";
                  version = "1.18.29";
                  sources = opencodeV1Sources;
                  archiveFormats = {
                    "aarch64-darwin" = "zip";
                    "aarch64-linux" = "tar";
                    "x86_64-linux" = "tar";
                  };
                })
                (mkOpenCode {
                  pname = "opencode-v2";
                  version = "2.0.10";
                  sources = opencodeV2Sources;
                  archiveFormats = {
                    "aarch64-darwin" = "tar-package";
                    "aarch64-linux" = "tar-package";
                    "x86_64-linux" = "tar-package";
                  };
                })
              ]
            else
              [ ];
          basePackages =
            with pkgs;
            [
              actionlint
              cargo-deny
            ]
            ++ rustToolchainPackages
            ++ [
              iii
              protobuf
              rust-analyzer
            ]
            ++ liveToolchainPackages;
          npm12 = pkgs.stdenvNoCC.mkDerivation {
            pname = "npm";
            version = "12.2.0";
            src = pkgs.fetchurl {
              url = "https://registry.npmjs.org/npm/-/npm-12.2.0.tgz";
              hash = "sha512-ZsJjKpTnlmSXOLLXiU1xDCzC4Wlok4IwZmh/aw2KUuXytU7q6qMv/cUT7MoeSf95Slwuw/lRXYefGzGCspHPNQ==";
            };
            nativeBuildInputs = [ pkgs.makeWrapper ];
            dontBuild = true;

            installPhase = ''
              runHook preInstall
              mkdir -p "$out/lib/node_modules/npm" "$out/bin"
              cp -R . "$out/lib/node_modules/npm/"
              makeWrapper ${pkgs.nodejs_24}/bin/node "$out/bin/npm" \
                --add-flags "$out/lib/node_modules/npm/bin/npm-cli.js"
              makeWrapper ${pkgs.nodejs_24}/bin/node "$out/bin/npx" \
                --add-flags "$out/lib/node_modules/npm/bin/npx-cli.js"
              runHook postInstall
            '';

            doInstallCheck = pkgs.stdenvNoCC.buildPlatform.canExecute pkgs.stdenvNoCC.hostPlatform;
            installCheckPhase = ''
              test "$("$out/bin/npm" --version)" = "12.2.0"
            '';
            meta.mainProgram = "npm";
          };
          mkShell =
            extraPackages:
            pkgs.mkShell {
              packages = basePackages ++ extraPackages;

              RUST_SRC_PATH =
                if system == "x86_64-darwin" then
                  "${rustToolchain}/lib/rustlib/src/rust/library"
                else
                  "${pkgs.rustPlatform.rustLibSrc}";
            };
        in
        {
          default = mkShell [ ];
          node24 = mkShell [
            npm12
            pkgs.nodejs_24
          ];
        }
      );
      packages = forAllSystems (
        system:
        let
          pkgs = import (if system == "x86_64-darwin" then nixpkgsDarwin else nixpkgs) {
            inherit system;
            config.allowDeprecatedx86_64Darwin = "force";
            overlays = nixpkgs.lib.optional (system == "x86_64-darwin") (import rust-overlay);
          };
          workerRustToolchain =
            if system == "x86_64-darwin" then pkgs.rust-bin.stable."1.98.1".minimal else null;
          workerRustPlatform =
            if system == "x86_64-darwin" then
              pkgs.makeRustPlatform {
                cargo = workerRustToolchain;
                rustc = workerRustToolchain;
              }
            else
              pkgs.rustPlatform;
          mkWorker =
            name:
            workerRustPlatform.buildRustPackage {
              pname = name;
              version = "0.1.0";
              src = cargoWorkspaceSource;
              strictDeps = true;

              cargoLock.lockFile = ./Cargo.lock;
              cargoBuildFlags = [
                "--package"
                name
              ];
              cargoTestFlags = [
                "--package"
                name
              ];

              nativeBuildInputs = [ pkgs.protobuf ];

              meta.mainProgram = name;
            };
          opencodeHarnessEventsSource = pkgs.lib.fileset.toSource {
            root = ./integrations/opencode-harness-events;
            fileset = pkgs.lib.fileset.unions [
              ./integrations/opencode-harness-events/LICENSE
              ./integrations/opencode-harness-events/README.md
              ./integrations/opencode-harness-events/package.json
              ./integrations/opencode-harness-events/src
              ./integrations/opencode-harness-events/tsconfig.build.json
              ./integrations/opencode-harness-events/tsconfig.json
            ];
          };
          opencodeHarnessEventsBuildTypes = pkgs.writeTextDir "bun-types/index.d.ts" ''
            declare module "@opencode-ai/plugin" {
              export type Plugin = (...args: never[]) => unknown;
              export type PluginInput = unknown;
            }
            declare module "@opencode/client" {
              export type V2Event = unknown;
            }
            declare module "@opencode/plugin/promise/plugin" {
              export type Cleanup = unknown;
              export type Context = unknown;
            }
          '';
          opencodeHarnessEvents = pkgs.stdenvNoCC.mkDerivation {
            pname = "opencode-harness-events";
            version = "0.1.0";
            src = opencodeHarnessEventsSource;
            strictDeps = true;

            nativeBuildInputs = [ (pkgs.typescript_5 or pkgs.typescript) ];

            buildPhase = ''
              runHook preBuild
              tsc --project tsconfig.build.json \
                --noCheck \
                --typeRoots ${opencodeHarnessEventsBuildTypes}
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              mkdir -p "$out"
              cp LICENSE README.md package.json "$out/"
              cp -R dist "$out/"
              runHook postInstall
            '';
          };
          piHarnessEvents = pkgs.stdenvNoCC.mkDerivation {
            pname = "pi-harness-events";
            version = "0.1.0";
            src = pkgs.lib.fileset.toSource {
              root = ./integrations/pi-harness-events;
              fileset = pkgs.lib.fileset.unions [
                ./integrations/pi-harness-events/LICENSE
                ./integrations/pi-harness-events/README.md
                ./integrations/pi-harness-events/package.json
                ./integrations/pi-harness-events/src
              ];
            };
            dontBuild = true;

            installPhase = ''
              runHook preInstall
              mkdir -p "$out"
              cp LICENSE README.md package.json "$out/"
              cp -R src "$out/"
              runHook postInstall
            '';
          };
          postgresql17Search = mkSearchPostgresql pkgs.postgresql_17;
          postgresql18Search = mkSearchPostgresql pkgs.postgresql_18;
          knowledgeGraphPostgresSmokeSource = pkgs.lib.fileset.toSource {
            root = ./.;
            fileset = pkgs.lib.fileset.unions [
              ./crates/knowledge-graph-store/migrations/0001_knowledge_graph_foundation.sql
              ./integration/knowledge-graph-postgres-smoke/scripts/setup.sh
              ./integration/knowledge-graph-postgres-smoke/scripts/rollback.sh
              ./integration/knowledge-graph-postgres-smoke/scripts/verify-schema.sh
              ./integration/knowledge-graph-postgres-smoke/scripts/verify-mutations.sh
              ./integration/knowledge-graph-postgres-smoke/scripts/verify-traversal.sh
            ];
          };
        in
        {
          harness-event-persistence = mkWorker "harness-event-persistence";
          harness-ingestion = mkWorker "harness-ingestion";
          mcp-worker = mkWorker "mcp-worker";
          memory-embedding = mkWorker "memory-embedding";
          session-post-processing = mkWorker "session-post-processing";
          session-post-processing-engine-fake = mkWorker "session-post-processing-engine-fake";
          opencode-harness-events = opencodeHarnessEvents;
          pi-harness-events = piHarnessEvents;
          postgresql17-search = postgresql17Search;
          postgresql18-search = postgresql18Search;
          postgresql17-search-verify = mkExtensionVerification pkgs postgresql17Search 17;
          postgresql18-search-verify = mkExtensionVerification pkgs postgresql18Search 18;
          postgresql17-search-fixture-verify = mkFixtureVerification pkgs postgresql17Search 17;
          postgresql18-search-fixture-verify = mkFixtureVerification pkgs postgresql18Search 18;
          postgresql17-memory-storage-verify = mkMemoryStorageVerification pkgs postgresql17Search 17;
          postgresql18-memory-storage-verify = mkMemoryStorageVerification pkgs postgresql18Search 18;
          postgresql17-memory-search-heads-verify =
            mkMemorySearchHeadsVerification pkgs postgresql17Search
              17;
          postgresql18-memory-search-heads-verify =
            mkMemorySearchHeadsVerification pkgs postgresql18Search
              18;
          postgresql17-memory-embedding-lifecycle-verify =
            mkMemoryEmbeddingLifecycleVerification pkgs postgresql17Search
              17;
          postgresql18-memory-embedding-lifecycle-verify =
            mkMemoryEmbeddingLifecycleVerification pkgs postgresql18Search
              18;
          postgresql17-memory-embedding-work-verify =
            mkMemoryEmbeddingWorkVerification pkgs postgresql17Search 17;
          postgresql18-memory-embedding-work-verify =
            mkMemoryEmbeddingWorkVerification pkgs postgresql18Search 18;
          postgresql17-memory-version-heads-verify =
            mkMemoryVersionHeadsVerification pkgs postgresql17Search
              17;
          postgresql18-memory-version-heads-verify =
            mkMemoryVersionHeadsVerification pkgs postgresql18Search
              18;
          postgresql17-memory-bm25-verify = mkMemoryBm25Verification pkgs postgresql17Search 17;
          postgresql18-memory-bm25-verify = mkMemoryBm25Verification pkgs postgresql18Search 18;
          postgresql17-memory-vector-verify = mkMemoryVectorVerification pkgs postgresql17Search 17;
          postgresql18-memory-vector-verify = mkMemoryVectorVerification pkgs postgresql18Search 18;
          postgresql17-session-post-processing-verify =
            mkSessionPostProcessingVerification pkgs pkgs.postgresql_17 17;
          postgresql18-session-post-processing-verify =
            mkSessionPostProcessingVerification pkgs pkgs.postgresql_18 18;
          postgresql17-knowledge-graph-schema-verify =
            mkKnowledgeGraphVerification pkgs knowledgeGraphPostgresSmokeSource
              pkgs.postgresql_17 17 "statement-capture" "schema" "verify-schema.sh";
          postgresql18-knowledge-graph-schema-verify =
            mkKnowledgeGraphVerification pkgs knowledgeGraphPostgresSmokeSource
              pkgs.postgresql_18 18 "row-change-publishing-disabled" "schema" "verify-schema.sh";
          postgresql17-knowledge-graph-mutation-verify =
            mkKnowledgeGraphVerification pkgs knowledgeGraphPostgresSmokeSource
              pkgs.postgresql_17 17 "statement-capture" "mutation" "verify-mutations.sh";
          postgresql18-knowledge-graph-mutation-verify =
            mkKnowledgeGraphVerification pkgs knowledgeGraphPostgresSmokeSource
              pkgs.postgresql_18 18 "row-change-publishing-disabled" "mutation" "verify-mutations.sh";
          postgresql17-knowledge-graph-traversal-verify =
            mkKnowledgeGraphVerification pkgs knowledgeGraphPostgresSmokeSource
              pkgs.postgresql_17 17 "statement-capture" "traversal" "verify-traversal.sh";
          postgresql18-knowledge-graph-traversal-verify =
            mkKnowledgeGraphVerification pkgs knowledgeGraphPostgresSmokeSource
              pkgs.postgresql_18 18 "row-change-publishing-disabled" "traversal" "verify-traversal.sh";
        }
      );
      checks = forAllSystems (_: { });
      apps = forAllSystems (
        system:
        let
          pkgs = import (if system == "x86_64-darwin" then nixpkgsDarwin else nixpkgs) {
            inherit system;
            config.allowDeprecatedx86_64Darwin = "force";
            overlays = nixpkgs.lib.optional (system == "x86_64-darwin") (import rust-overlay);
          };
          mkPostgreSQLMemoryHistoryVerificationApp =
            major:
            let
              name = "postgresql${toString major}-memory-history-mcp-verify";
              postgresPackage = "postgresql${toString major}-search";
              program = pkgs.writeShellScriptBin name ''
                set -euo pipefail

                if [[ ! -f "$PWD/flake.nix" || ! -f "$PWD/Cargo.toml" ]]; then
                  printf 'run this verifier from the repository root\n' >&2
                  exit 64
                fi

                postgresql="$(nix build --no-link --print-out-paths .#${postgresPackage})"
                export MEMORY_SCHEMA_MIGRATION="$PWD/crates/memory-store/migrations/0001_memory_schema_search.sql"
                exec nix develop --command \
                  bash integration/memory-postgres-smoke/scripts/setup.sh \
                    "$postgresql/bin" ${toString major} -- \
                    bash integration/memory-postgres-smoke/scripts/verify-memory-history-mcp.sh
              '';
            in
            {
              type = "app";
              program = "${program}/bin/${name}";
            };
        in
        {
          postgresql17-memory-history-mcp-verify = mkPostgreSQLMemoryHistoryVerificationApp 17;
          postgresql18-memory-history-mcp-verify = mkPostgreSQLMemoryHistoryVerificationApp 18;
        }
      );
    };
}
