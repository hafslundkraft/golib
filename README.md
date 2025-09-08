# Golib

A collection of Hafslund Go libraries.

As this repo is internal, some configuration is needed both for local development and for CI:

## Local development

Run
```shell
go env -w GOPRIVATE=github.com/HafslundEcoVannkraft
```
to tell Go not to try public proxies to for our internal packages.

Then, add
```
[url "git@github.com:HafslundEcoVannkraft/"]
    insteadOf = https://github.com/HafslundEcoVannkraft/
```
to your `~/.gitconfig`.

Finally, make sure you have an ssh agent running:
```shell
eval $(ssh-agent)
ssh-add ~/.ssh/id_ed25519 # Or wherever your ssh key is
```

## Docker
Docker builds need the `--ssh=default` flag to forward the ssh agent to the build
container, and then you have to explicitly mount the ssh socket and set up the git config
in the Dockerfile:

```dockerfile
RUN cat <<EOF >> ~/.gitconfig
[url "git@github.com:HafslundEcoVannkraft/"]
    insteadOf = https://github.com/HafslundEcoVannkraft/
EOF
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN go env -w GOPRIVATE=github.com/HafslundEcoVannkraft

RUN --mount=type=ssh go mod download
```

## From Github Actions

You'll need to use the Github organization secret GOLIB_SSH_KEY to access the
repo in CI. The relevant parts:

```
...
    steps:
      - name: Install SSH key
        run: |
          mkdir ~/.ssh
          echo "${{ secrets.GOLIB_SSH_KEY }}" >> ~/.ssh/id_golib
          chmod 600 ~/.ssh/id_golib
          eval $(ssh-agent)
          echo "SSH_AUTH_SOCK=$SSH_AUTH_SOCK" >> $GITHUB_ENV
          ssh-add ~/.ssh/id_golib
      - name: Build and push
        id: build
        uses: HafslundEcoVannkraft/actions/build@main
        with:
          app-name: <app>
          system-name: <system>
          dockerfile: Dockerfile
          client-id: ${{ vars.HAPPI_GHA_CLIENT_ID }}
          ssh: default
...
```