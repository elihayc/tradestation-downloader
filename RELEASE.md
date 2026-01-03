# Release Instructions

## Steps to publish a new version to PyPI

### 1. Update version number

Edit `pyproject.toml` and update the version:
```toml
version = "1.1.0"  # Change to new version
```

### 2. Commit and tag

```bash
git add pyproject.toml
git commit -m "Bump version to 1.1.0"
git tag v1.1.0
git push && git push --tags
```

### 3. Clean old builds

```bash
rm -rf dist/
```

### 4. Build package

```bash
.venv\Scripts\python.exe -m build
```

### 5. Upload to PyPI

```bash
.venv\Scripts\python.exe -m twine upload dist/* -u __token__ -p pypi-YOUR_TOKEN_HERE
```

Or without token in command (will prompt):
```bash
.venv\Scripts\python.exe -m twine upload dist/*
# Username: __token__
# Password: pypi-YOUR_TOKEN_HERE
```

## PyPI Token

- Create/manage tokens at: https://pypi.org/manage/account/token/
- Tokens start with `pypi-`
- Store securely, do not commit to git

## Verify

After upload, check: https://pypi.org/project/tradestation-downloader/
