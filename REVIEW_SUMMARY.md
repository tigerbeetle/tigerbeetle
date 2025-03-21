# Review Summary

This PR adds a `git-review` command for doing offline-first reviews in git itself.

The command is _very_ rough and intentionally does only the bare minimum. I want to start using it
in anger before adding bells and whistles. Please refer to `git-review.zig` for docs aka
implementation details.

To review this particular PR:

* Clone the relevant branch (ideally, in a dedicated review work tree):

    ```
    $ git fetch
    $ git clone matklad/git-review
    ```
* Add your review comments in-code, starting each comment with `//?` marker.
* Amend the top review commit with your changes
* `git push --force-with-lease` it.

I then will pull your review, address it, resolve all comments marking them with `//? resolved.` and
will push my changes with `git push --force-with-lease` also.

Once you are happy with the state of the pull request, run

```
./zig/zig build git-review -- lgtm
```

to push a reverting commit upstream, and then leave an approving review on GitHub.
