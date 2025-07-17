{
  description = "A pure dev environment for Exanda";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        # Versions!!!
        elixir_version = "elixir_1_18";
        erlang_version = "erlang_28";

        pkgs = nixpkgs.legacyPackages.${system};
      in
      with pkgs;
      {
        devShells.default = mkShell {
          buildInputs =
            [
              bash
              beam.packages.${erlang_version}.${elixir_version}
              inotify-tools
              pkgs.${erlang_version}
            ]
            ++ lib.optionals stdenv.isLinux [
              libnotify
              inotify-tools
            ]
            ++ lib.optionals stdenv.isDarwin [
              terminal-notifier
              darwin.apple_sdk.frameworks.CoreFoundation
              darwin.apple_sdk.frameworks.CoreServices
            ];

          shellHook = ''
            mkdir -p .nix-mix
            mkdir -p .nix-hex
            export MIX_HOME=$PWD/.nix-mix
            export HEX_HOME=$PWD/.nix-hex
            export ERL_AFLAGS='-kernel shell_history enabled'
            mix local.hex --force --if-missing
            mix local.rebar --force --if-missing
          '';

        };
      }
    );
}
