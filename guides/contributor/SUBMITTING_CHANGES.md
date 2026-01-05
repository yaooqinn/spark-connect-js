# Submitting Changes

This guide explains how to contribute code to spark.js through pull requests.

## Before You Start

1. **Check existing issues** - Look for related issues or discussions
2. **Discuss major changes** - Open an issue for significant features before implementing
3. **Review guidelines** - Read [Code Style Guide](CODE_STYLE.md) and [Build and Test](BUILD_AND_TEST.md)

## Contribution Workflow

### 1. Fork and Branch

```bash
# Fork the repository on GitHub first, then clone your fork
git clone https://github.com/YOUR_USERNAME/spark.js.git
cd spark.js

# Add upstream remote
git remote add upstream https://github.com/yaooqinn/spark.js.git

# Create a feature branch
git checkout -b feature/my-feature
# or
git checkout -b fix/issue-123
```

### 2. Make Changes

Follow these guidelines:

- **Follow code style** - See [Code Style Guide](CODE_STYLE.md)
- **Add Apache license header** - All new files must include the Apache 2.0 header
- **Write tests** - Add tests for new functionality
- **Update documentation** - Update JSDoc comments and guides if needed
- **Keep commits atomic** - One logical change per commit

### 3. Test Your Changes

```bash
# 1. Run linting
npm run lint

# 2. Type check
npx tsc --noEmit

# 3. Start Spark Connect server
docker build -t scs .github/docker  # First time only
docker run --name sparkconnect -p 15002:15002 -d scs

# Wait 15-20 seconds, then run tests
npm test

# 4. Clean up
docker stop sparkconnect && docker rm sparkconnect
```

### 4. Commit Changes

Write clear, descriptive commit messages:

```bash
git add .
git commit -m "feat: Add support for DataFrame.dropDuplicates

- Implement dropDuplicates with column subset
- Add tests for various scenarios
- Update DataFrame API documentation

Closes #123"
```

#### Commit Message Format

Follow this pattern:

```
<type>: <subject>

<body>

<footer>
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, no logic change)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Example**:

```
feat: Add DataFrame.pivot operation

Implement pivot operation for DataFrame transformations:
- Add pivot() method to DataFrame class
- Support multiple aggregation functions
- Handle null values correctly

Tests include:
- Basic pivot with single aggregation
- Multiple value columns
- Null handling

Closes #456
```

### 5. Push Changes

```bash
# Push to your fork
git push origin feature/my-feature
```

### 6. Create Pull Request

1. Go to your fork on GitHub
2. Click "Pull Request" button
3. Select your feature branch
4. Fill out the PR template

## Pull Request Guidelines

### PR Title

Use clear, descriptive titles:

```
✅ Good:
- Add DataFrame.pivot operation
- Fix memory leak in gRPC client
- Update documentation for DataFrameWriter

❌ Avoid:
- Update
- Fix bug
- Changes
```

### PR Description

Include:

1. **What**: What does this PR do?
2. **Why**: Why is this change needed?
3. **How**: How does it work?
4. **Testing**: How was it tested?
5. **Related Issues**: Link to related issues

**Template**:

```markdown
## Description

Brief description of the change.

## Motivation

Why is this change necessary? What problem does it solve?

## Changes

- Change 1
- Change 2
- Change 3

## Testing

- [ ] Added unit tests
- [ ] All tests pass locally
- [ ] Linting passes
- [ ] Tested manually (if applicable)

## Checklist

- [ ] Follows code style guidelines
- [ ] Added Apache license header to new files
- [ ] Updated documentation
- [ ] Added tests for new functionality
- [ ] All CI checks pass

## Related Issues

Closes #123
```

### PR Size

Keep PRs focused and manageable:

- **Small PR**: < 200 lines (preferred)
- **Medium PR**: 200-500 lines (acceptable)
- **Large PR**: > 500 lines (break into smaller PRs if possible)

For large features, consider:
1. Breaking into multiple PRs
2. Using feature flags
3. Incremental implementation

## Code Review Process

### What Reviewers Look For

1. **Correctness** - Does the code work as intended?
2. **Tests** - Are there adequate tests?
3. **Style** - Does it follow project conventions?
4. **Documentation** - Is it well-documented?
5. **Performance** - Are there any performance concerns?
6. **Security** - Are there security implications?

### Responding to Reviews

- **Be responsive** - Reply to comments promptly
- **Be open** - Consider feedback objectively
- **Ask questions** - If something is unclear, ask
- **Make changes** - Address feedback in new commits
- **Discuss disagreements** - Explain your reasoning politely

### Making Review Changes

```bash
# Make requested changes
git add .
git commit -m "fix: Address review feedback

- Improve error handling
- Add additional test cases
- Update documentation"

# Push to update PR
git push origin feature/my-feature
```

## CI/CD Checks

Your PR must pass these automated checks:

### 1. Linter Workflow

Runs ESLint on all TypeScript files:

```bash
npm run lint
```

**Fix linting issues**:
```bash
npx eslint --fix src/
```

### 2. CI Workflow

Runs full test suite with Spark Connect server:

```bash
npm test
```

**All tests must pass.**

### 3. Documentation Workflow

Generates API documentation from JSDoc comments:

```bash
npm run docs
```

**Verify documentation builds** if you changed JSDoc comments.

## Keeping Your Branch Updated

### Sync with Upstream

```bash
# Fetch upstream changes
git fetch upstream

# Rebase your branch
git checkout feature/my-feature
git rebase upstream/main

# Force push (only for feature branches)
git push origin feature/my-feature --force-with-lease
```

### Resolving Conflicts

```bash
# During rebase, if conflicts occur:
# 1. Edit conflicted files
# 2. Mark as resolved
git add .
git rebase --continue

# If rebase becomes messy, abort and try merge:
git rebase --abort
git merge upstream/main
```

## After Your PR is Merged

### Clean Up

```bash
# Delete local branch
git checkout main
git branch -D feature/my-feature

# Delete remote branch
git push origin --delete feature/my-feature

# Update your main branch
git pull upstream main
git push origin main
```

### Celebrate! 🎉

Your contribution is now part of spark.js! Thank you for contributing!

## Common Issues

### CI Fails But Tests Pass Locally

**Possible causes**:
1. Different Node.js version - CI uses Node 23
2. Timing issues - Tests may be flaky
3. Missing await - Async operations not properly awaited

**Solution**:
- Check CI logs for specific error
- Run tests multiple times locally
- Ensure using same Node version as CI

### Merge Conflicts

**Solution**:
```bash
# Update from upstream
git fetch upstream
git rebase upstream/main

# Resolve conflicts
# ... edit files ...
git add .
git rebase --continue

# Force push
git push origin feature/my-feature --force-with-lease
```

### Lint Errors Only in CI

**Possible causes**:
1. Different ESLint version
2. Generated files not ignored

**Solution**:
```bash
# Install exact dependency versions
npm ci

# Run lint locally
npm run lint
```

### PR Review Takes Too Long

**What to do**:
1. Be patient - Reviewers are volunteers
2. Ping in comments after a few days
3. Ensure PR is ready (CI passes, well-described)
4. Make sure you're following guidelines

## Best Practices

### Do's ✅

- **Write clear commit messages** - Explain what and why
- **Add tests** - Cover new functionality
- **Keep PRs focused** - One feature/fix per PR
- **Update documentation** - Keep docs in sync
- **Respond to feedback** - Be collaborative
- **Be patient** - Reviews take time

### Don'ts ❌

- **Don't commit generated files** - Except `src/gen/` (protobuf)
- **Don't commit build artifacts** - `dist/`, `coverage/`
- **Don't commit logs** - `logs/` directory
- **Don't commit credentials** - No API keys or passwords
- **Don't ignore CI failures** - Fix issues before requesting review
- **Don't force push to main** - Only force push to feature branches

## Advanced Git Tips

### Interactive Rebase

Clean up commits before submitting:

```bash
# Rebase last 3 commits
git rebase -i HEAD~3

# In editor:
# pick -> Keep commit as is
# reword -> Change commit message
# squash -> Combine with previous commit
# drop -> Remove commit
```

### Cherry-Pick Commits

Apply specific commits to your branch:

```bash
git cherry-pick <commit-hash>
```

### Stash Changes

Save work in progress:

```bash
# Stash changes
git stash

# List stashes
git stash list

# Apply most recent stash
git stash pop
```

## Getting Help

If you need help:

1. **Check documentation** - Review contributor guides
2. **Search issues** - Look for similar problems
3. **Ask questions** - Comment on your PR or open a discussion
4. **Be specific** - Provide error messages, logs, and context

## Resources

- [GitHub Flow](https://guides.github.com/introduction/flow/) - Git workflow
- [Conventional Commits](https://www.conventionalcommits.org/) - Commit message format
- [Code Review Guidelines](https://google.github.io/eng-practices/review/) - Google's code review practices
- [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) - Project license

Thank you for contributing to spark.js! 🚀
