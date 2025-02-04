# Contributing to Ns4Kafka

Welcome and thank you for considering contributing to Ns4kafka!

By following these guidelines, you can help make the contribution process easy and effective for everyone involved. It also shows that you agree to respect the time of the developers managing and developing these open source projects. In return, we will reciprocate that respect by addressing your issue, assessing changes, and helping you finalize your pull requests.

## Getting Started

### Issues

Issues should be used to report problems, request a new feature, or to discuss potential changes before a PR is created. When you create a new Issue, a template will be loaded that will guide you through collecting and providing the information we need to investigate.

If you find an existing issue that addresses the problem you're having, please add your own reproduction information to the existing issue instead of creating a new one. Adding a [reaction](https://github.blog/2016-03-10-add-reactions-to-pull-requests-issues-and-comments/) can also indicate to our maintainers that a particular problem is affecting more than just the reporter.

If you're unable to find an open issue addressing the problem, open a new one. Be sure to include a title and a clear description, relevant information, and a code sample or executable test case demonstrating the expected behavior that is not occurring.

### Pull Requests

PRs are always welcome and can be a quick way to get your fix or improvement slated for the next release. In general, PRs should:

- Only fix/add the functionality in question OR address wide-spread style issues, not both.
- Add unit or integration tests for fixed or changed functionality (if a test suite already exists).
- Address a single concern in the least number of changed lines as possible.
- Be accompanied by a complete Pull Request template (loaded automatically when a PR is created).

Be sure to use the past tense ("Added new feature...", "Fixed bug on...") and add tags to the PR ("documentation" for documentation updates, "bug" for bug fixing, etc.).

For changes that address core functionality or would require breaking changes (e.g. a major release), it's best to open an Issue to discuss your proposal first. This is not required but can save time creating and reviewing changes.

In general, we follow the ["fork-and-pull" Git workflow](https://github.com/susam/gitpr)

- Fork the repository to your own Github account
- Clone the project to your machine
- Create a branch locally from master with a succinct but descriptive name
- Commit changes to the branch
- Following any formatting and testing guidelines specific to this repo
- Push changes to your fork
- Open a PR in our repository targeting master and follow the PR template so that we can efficiently review the changes.

## Styleguides

### Git Commit Messages

When contributing to the project, it's important to follow a consistent style for Git commit messages. Here are some guidelines to keep in mind:

- Use the present tense, such as "Add feature," rather than the past tense, such as "Added feature."
- Use the imperative mood, such as "Move cursor to..." rather than "Moves cursor to..."
- Limit the first line of the commit message to 72 characters or less.
- Use references to issues and pull requests after the first line as needed.
- If your commit only changes documentation, include `[ci skip]` in the commit title.
