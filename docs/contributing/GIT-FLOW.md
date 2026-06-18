# Git Flow:

[Git Flow]: https://nvie.com/posts/a-successful-git-branching-model/
[Semantic Versioning]: https://semver.org

> [!IMPORTANT]
In the following, **X**, **Y** and **Z** refer to **MAJOR**, **MINOR** and **PATCH** of [Semantic Versioning].

[Git Flow] is a branching strategy that defines dedicated branches for features, releases, and hotfixes, built around long-lived main and develop branches. It’s well suited for projects with long or complex release cycles, or when teams need an unstable shared branch for ongoing development.

These are the 5 types of branches used in this strategy:
| Name            | Type      | Purpose                                                                          |
|-----------------|-----------|----------------------------------------------------------------------------------|
| `main`          | Permanent | Represents the production-ready state; all releases originate here.              |
| `develop`       | Permanent | The integration branch for ongoing development; features are merged here.        |
| `feature/*`     | Temporary | Branches for developing new features, branched off from `develop`.               |
| `release/X.Y.Z` | Temporary | Branches for preparing a new production release, branched off from `develop`.    |
| `hotfix/*`      | Temporary | Branches for critical fixes to the production version, branched off from `main`. |

<div align="justify">

### **Feature workflow**:

1. Create a branch from `develop`.
2. Work on the feature.
3. Rebase on-top of `develop`.
4. Push changes and open a PR. Ask for a review.
5. Merge branch to `develop` with a merge commit.

### **Release workflow**:

1. Create a branch named `release/X.Y.Z` from `develop`.
2. Perform all steps needed to make the release.
3. Push changes and open a PR. Ask for a review.
4. Merge `release/X.Y.Z` to `main` and also to `develop`.
5. Create a release from `main`. The tag should be named `vX.Y.Z`.

### `Hotfix workflow`:

1. Create a branch named `hotfix/your-branch-name` from `main`.
2. Work on the fix. Perform steps needed to make the release.
3. Push changes and open a PR. Ask for a review.
4. Merge `hotfix/your-branch-name` to `main` and also to `develop`.
5. Create a release from `main`. The tag should be named `vX.Y.Z`.

</div>
