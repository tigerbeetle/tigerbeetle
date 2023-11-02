# docs.tigerbeetle.com

This site is built with a pretty vanilla docusaurus configuration and
hosted on Github Pages.

## Actual docs

The actual docs themselves are in the [TigerBeetle
docs/](https://github.com/tigerbeetledb/tigerbeetle/tree/main/docs)
directory.

They are copied here during the build step.

## Building for deployment

The built assets are *committed* by a developer (no CI does this at the
moment).

The built assets are in the [docs/](./docs/) directory since this is
where Github Pages wants them to be.

To build:

```bash
$ ./scripts/build.sh
```

Then commit all the changes (in the `docs/` directory).
