# Contributing guidelines

1. All changes must be covered with tests (ideally unit + integration)
2. All commits must follow the [conventional commit][1] format
3. All changes must pass the workflow tests
4. If a bug is fixed, a test should be added to ensure regression doesn't appear

## Local testing

This project uses [`uv`][2]. To set up the project locally, use

```bash
uv pip install .
```

To test, you also need development tools, add them to your local environment
using this command:

```bash
uv pip install --group dev
```

Then you can use [`tox`][3] to run linting and unit tests.

For integration tests you also need [`podman`][4] or [`docker`][5] with
`compose`. Run:

```bash
podman compose up -d 
```

Wait a while and then run:

```bash
tox -e integration
```

Don't forget to clean up the test environment afterward. Use

```bash
podman compose down
```

to do that (or switch `podman` with `docker` depending on your tool of choice).

## Releasing

After each contribution to the main branch, an automatic workflow runs to check
if any changes with conventional commit messages were added. If yes, this
workflow creates a pull request with proposed changelog.

To create a new tag, simply merge the automated pull request. This tag is
automatically released to GitHub releases.

To release a new version to PyPI, go to Actions and select "release". Run this
action on your newly created tag.

> NOTE: Do not run PyPI releases on main branch. Always use Git tags!


[1]: https://www.conventionalcommits.org/en/v1.0.0-beta.2/
[2]: https://docs.astral.sh/uv/
[3]: https://tox.wiki/en/4.32.0/
[4]: https://podman.io/
[5]: https://www.docker.com/