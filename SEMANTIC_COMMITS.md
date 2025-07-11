# Semantic Commit Messages

This document explains how to structure commit messages in a semantic way for the OpenCitations Meta project. Following these conventions ensures that automatic versioning and changelog generation work correctly.

## Commit Message Format

Each commit message consists of a **header**, a **body**, and a **footer**. The header has a special format that includes a **type**, a **scope**, and a **subject**:

```
<type>(<scope>): <subject>
<BLANK LINE>
<body>
<BLANK LINE>
<footer>
```

The **header** is mandatory and the **scope** of the header is optional. The **body** and **footer** are also optional.

### Types

The commit type must be one of the following:

- **feat**: A new feature
- **fix**: A bug fix
- **docs**: Documentation only changes
- **style**: Changes that do not affect the meaning of the code (white-space, formatting, etc.)
- **refactor**: A code change that neither fixes a bug nor adds a feature
- **perf**: A code change that improves performance
- **test**: Adding missing tests or correcting existing tests
- **build**: Changes that affect the build system or external dependencies
- **ci**: Changes to our CI configuration files and scripts
- **chore**: Other changes that don't modify src or test files

### Scope

The scope should be the name of the module affected (as perceived by the person reading the changelog generated from commit messages).

### Subject

The subject contains a succinct description of the change:

- Use the imperative, present tense: "change" not "changed" nor "changes"
- Don't capitalize the first letter
- No dot (.) at the end

### Body

The body should include the motivation for the change and contrast this with previous behavior.

### Footer

The footer should contain any information about **Breaking Changes** and is also the place to reference GitHub issues that this commit **Closes**.

**Breaking Changes** should start with the word `BREAKING CHANGE:` with a space or two newlines. The rest of the commit message is then used for this.

## Examples

### Feature Commit

```
feat(parser): add ability to parse arrays

This adds support for parsing arrays in CSV files, which was previously not supported.

Closes #123
```

### Bug Fix Commit

```
fix(validator): correct DOI validation pattern

The previous regex pattern was incorrectly rejecting valid DOIs that contained certain special characters.

Fixes #456
```

### Documentation Commit

```
docs(readme): update installation instructions

Updated the installation section to include Poetry as the recommended installation method.
```

### Breaking Change Commit

```
feat(api): change response format for /metadata endpoint

BREAKING CHANGE: The response format has changed from XML to JSON. Clients will need to update their parsers.
```

## Triggering Releases

To trigger a release, include `[release]` in your commit message. For example:

```
feat(core): implement new metadata extraction algorithm [release]

This commit introduces a completely new algorithm for extracting metadata from PDFs, resulting in 30% higher accuracy.
```

When this commit is pushed to the master branch, it will automatically trigger the release workflow, which will:

1. Run all tests
2. Create a new semantic version based on commit messages
3. Generate a changelog
4. Create a GitHub release
5. Build and publish the package to PyPI

## Version Bumping

The version number will be automatically bumped based on the commit types:

- **patch** (e.g., 1.0.0 → 1.0.1): `fix`, `perf`, `style`, `docs`, `refactor`, `test`, `build`, `ci`, `chore`
- **minor** (e.g., 1.0.0 → 1.1.0): `feat`
- **major** (e.g., 1.0.0 → 2.0.0): Any commit with `BREAKING CHANGE:` in the footer

## References

- [Conventional Commits](https://www.conventionalcommits.org/)
- [Semantic Versioning](https://semver.org/)
- [Angular Commit Message Guidelines](https://github.com/angular/angular/blob/master/CONTRIBUTING.md#commit)
