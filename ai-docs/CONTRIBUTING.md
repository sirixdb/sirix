# Contributing

From opening a bug report to creating a pull request: every contribution is
appreciated and welcome. If you're planning to implement a new feature or change
the api please create an issue first. This way we can ensure that your precious
work is not in vain.

## Submitting Changes

After getting some feedback, push to your fork and submit a pull request. We
may suggest some changes or improvements or alternatives, but for small changes
your pull request should be accepted quickly.

Some things to consider:

* Write tests
* Follow the existing coding style
* Write a [good commit message](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html)

## Setup your fork

Fork this repository! Once you have a copy of this repo on your own account, clone this repo to your computer by typing in something like:

1. `git clone https://github.com/sirixdb/sirix.git`

  (Replace the URL with your own repository URL path.)

2. Run `cd sirix`. Then, set up this repository as an upstream branch using:
  * `git remote add upstream https://github.com/sirixdb/sirix.git`

  Now, whenever you want to sync with the owner repository. Do the following:
  * `git fetch upstream`
  * `git checkout staging`
  * `git merge upstream/staging`

  Note: You can type `git remote -v` to check which repositories your `origin` and `upstream` are pointing to.
