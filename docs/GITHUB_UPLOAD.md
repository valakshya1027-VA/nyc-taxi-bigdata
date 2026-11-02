# Uploading this project to GitHub

## Option A: command line (recommended)

1. Create an empty repository on github.com (New repository). Do NOT add a README or .gitignore there.
2. In the project folder:

```bash
git init
git add .
git commit -m "Initial commit: NYC taxi big data analytics project"
git branch -M main
git remote add origin https://github.com/<your-username>/nyc-taxi-bigdata.git
git push -u origin main
```

If asked for a password, use a Personal Access Token (GitHub > Settings > Developer settings > Personal access tokens) or sign in through GitHub CLI / Git Credential Manager.

## Option B: GitHub CLI

```bash
gh auth login
gh repo create nyc-taxi-bigdata --public --source=. --remote=origin --push
```

## Option C: web upload (no Git installed)

1. Create the repository on github.com.
2. Click "uploading an existing file", unzip the project, drag all files and folders in, and commit.
   (The web uploader accepts up to 100 files at a time and files under 25 MB.)

## After uploading

- Add the repo description and topics: `pyspark`, `big-data`, `machine-learning`, `nyc-taxi`, `streamlit`.
- Add screenshots of the dashboard to `docs/` and link them in the README.
- Replace `[Your Name]` in LICENSE and the placeholder results table in README.
- Never commit the raw data files; `.gitignore` already excludes them.
