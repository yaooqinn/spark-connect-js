# Release Process

Guide to releasing new versions of spark.js.

## Overview

This document describes the process for releasing new versions of spark.js to npm.

**Versioning**: spark.js follows [Semantic Versioning](https://semver.org/) (semver):
- **Major** (X.0.0): Breaking changes
- **Minor** (0.X.0): New features, backward compatible
- **Patch** (0.0.X): Bug fixes, backward compatible

## Prerequisites

### Required Access

- **npm publish rights**: Must be a collaborator on npm
- **GitHub repository access**: Push access to create tags
- **Verified npm account**: 2FA enabled

### Required Tools

- Node.js 18.x or higher
- npm 8.x or higher
- Git access to repository

## Release Checklist

Before releasing:

- [ ] All tests pass: `npm test`
- [ ] Linter passes: `npm run lint`
- [ ] Documentation builds: `npm run docs`
- [ ] CI/CD passes on main branch
- [ ] CHANGELOG.md is updated
- [ ] Version number is decided
- [ ] Dependencies are up to date
- [ ] No security vulnerabilities: `npm audit`

## Version Planning

### Determine Version Number

**Patch release (0.0.X)**: Bug fixes only
- Fix for issue #123
- Typo corrections
- Performance improvements (no API changes)

**Minor release (0.X.0)**: New features
- New DataFrame methods
- New SQL functions
- New configuration options
- Deprecations (with backward compatibility)

**Major release (X.0.0)**: Breaking changes
- API redesign
- Removed deprecated features
- Changed behavior of existing features
- Updated dependencies with breaking changes

### Current Version

Check current version:

```bash
npm version
# or
cat package.json | grep version
```

## Release Steps

### 1. Update Version Number

Use npm version command:

```bash
# Patch release (0.1.0 → 0.1.1)
npm version patch

# Minor release (0.1.0 → 0.2.0)
npm version minor

# Major release (0.1.0 → 1.0.0)
npm version major
```

This automatically:
- Updates `package.json`
- Updates `package-lock.json`
- Creates a git commit
- Creates a git tag

### 2. Update CHANGELOG.md

Add release notes to CHANGELOG.md:

```markdown
## [0.2.0] - 2024-01-15

### Added
- New `withColumnRenamed()` method for DataFrame
- Support for window functions
- Additional SQL functions (rank, dense_rank, etc.)

### Changed
- Improved error messages for connection failures
- Updated dependencies to latest versions

### Fixed
- Fixed issue #123: Null handling in aggregations
- Fixed type inference for complex types

### Deprecated
- `oldMethod()` is deprecated, use `newMethod()` instead
```

### 3. Review Changes

```bash
# Check what will be published
npm pack --dry-run

# View package contents
npm pack
tar -xvzf spark.js-0.2.0.tgz
cd package
ls -la
cd ..
rm -rf package spark.js-0.2.0.tgz
```

### 4. Build and Test

```bash
# Install dependencies
npm install

# Run linter
npm run lint

# Build
npm run build

# Run tests
npm test

# Generate docs
npm run docs
```

All must pass before releasing.

### 5. Commit and Push

```bash
# Commit version changes
git add package.json package-lock.json CHANGELOG.md
git commit -m "Release v0.2.0"

# Push commits and tags
git push origin main
git push origin --tags
```

### 6. Publish to npm

```bash
# Publish to npm
npm publish

# For pre-release versions
npm publish --tag beta
```

**Note**: The `prepublishOnly` script in package.json automatically runs `npm run build`.

### 7. Create GitHub Release

1. Go to https://github.com/yaooqinn/spark.js/releases
2. Click "Draft a new release"
3. Choose the tag (e.g., `v0.2.0`)
4. Set release title: `v0.2.0`
5. Copy content from CHANGELOG.md
6. Publish release

## Pre-release Versions

For beta or RC versions:

```bash
# Create pre-release version
npm version 0.2.0-beta.1

# Publish with tag
npm publish --tag beta

# Users install with
npm install spark.js@beta
```

## Hotfix Releases

For urgent fixes to production:

1. Create hotfix branch from tag:
   ```bash
   git checkout -b hotfix/0.1.1 v0.1.0
   ```

2. Make and test fix

3. Version and publish:
   ```bash
   npm version patch
   git push origin hotfix/0.1.1
   git push origin --tags
   npm publish
   ```

4. Merge back to main:
   ```bash
   git checkout main
   git merge hotfix/0.1.1
   git push origin main
   ```

## Post-Release

### Verify Publication

```bash
# Check on npm
npm view spark.js

# Check specific version
npm view spark.js@0.2.0

# Test installation
mkdir test-install
cd test-install
npm install spark.js@0.2.0
cd ..
rm -rf test-install
```

### Announcement

Announce the release:
- Update GitHub Release notes
- Post in discussions/community channels
- Update documentation website

### Update Dependencies

For projects using spark.js:

```bash
npm update spark.js
```

## Rollback

If a release has issues:

### Deprecate Version on npm

```bash
# Mark version as deprecated
npm deprecate spark.js@0.2.0 "This version has a critical bug, use 0.2.1 instead"
```

**Note**: Cannot unpublish after 72 hours or if other packages depend on it.

### Release Fix Version

1. Fix the issue
2. Release patch version (e.g., 0.2.1)
3. Announce the fix

## Version Strategy

### Pre-1.0 Releases

Current status: **Experimental** (version 0.x.x)

During pre-1.0:
- Major version is 0
- Minor version may include breaking changes
- Move carefully toward stable 1.0

### Moving to 1.0

Requirements for 1.0 release:
- [ ] Core API is stable
- [ ] Well-tested (>80% coverage)
- [ ] Documentation complete
- [ ] No known critical bugs
- [ ] Production usage validated
- [ ] Performance acceptable
- [ ] All features documented

### Post-1.0 Releases

After 1.0:
- Follow strict semantic versioning
- Maintain backward compatibility for minor versions
- Deprecate before removing features
- Support LTS versions if needed

## Best Practices

1. **Test thoroughly**: Run full test suite before release
2. **Update docs**: Keep documentation in sync with code
3. **Communicate changes**: Clear release notes in CHANGELOG
4. **Follow semver**: Respect semantic versioning rules
5. **Review dependencies**: Keep dependencies updated and secure
6. **Automate when possible**: Use CI/CD for validation
7. **Tag releases**: Always create git tags
8. **Sign commits**: Use GPG signing for releases
9. **Backup**: Have rollback plan ready

## Automation

### GitHub Actions Release Workflow

Create `.github/workflows/release.yml`:

```yaml
name: Release

on:
  push:
    tags:
      - 'v*'

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Node.js
        uses: actions/setup-node@v3
        with:
          node-version: '18'
          registry-url: 'https://registry.npmjs.org'
      
      - name: Install dependencies
        run: npm install
      
      - name: Run tests
        run: npm test
      
      - name: Build
        run: npm run build
      
      - name: Publish to npm
        run: npm publish
        env:
          NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}
      
      - name: Create GitHub Release
        uses: actions/create-release@v1
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        with:
          tag_name: ${{ github.ref }}
          release_name: Release ${{ github.ref }}
          draft: false
          prerelease: false
```

## Support Policy

### Supported Versions

- **Current minor version**: Full support
- **Previous minor version**: Security fixes only
- **Older versions**: No support

Example: If 0.3.0 is current:
- 0.3.x: Full support
- 0.2.x: Security fixes
- 0.1.x: No support

### Long-Term Support (LTS)

Not currently applicable for pre-1.0 versions.

Post-1.0, consider:
- LTS versions for enterprise users
- Extended support timelines
- Backporting critical fixes

## Resources

- [Semantic Versioning](https://semver.org/)
- [npm Publishing Guide](https://docs.npmjs.com/cli/v8/commands/npm-publish)
- [Keep a Changelog](https://keepachangelog.com/)

## Next Steps

- Review [Building](building.md) for build process
- See [Testing](testing.md) for validation
- Check [Contributing](../contributing/README.md) for contribution process
