# Review Summary

Even though it will not be directly available to clients, we should probably test the `get_events` operation in the VOPR, as it is important for correctness.
Also, I think we should have some sort of integration test which includes RabbitMQ before this is merged.

## File spec_parser.py

Ln 8
```
#//? dj: Please also add a permalink which references the exact commit hash of the XML
#//? file that you used.
#//? resolved.
```

Ln 78
//? dj: Can we assert that this tag matches?
//? resolved.

Ln 87
```
//? dj: I don't have a preference, but note that you can also write:
//?    print(
//?        f"..."
//?        f"..."
//?        f"...")
//? I guess multiple print statements are needed anyway for conditionals/loops
//? so maybe it is fine as-is!
//? batiati: It's visually cleaner, but inconvenient for handling line breaks.
//?    print(
//?        f"...\n" <- add \n.
//?        f"...\n" <- add \n.
//?        f"...")  <- not in this last one!
//? Multiline strings with triple quotes aren't great for indentation either!
//? At least multiple prints are consistent!
//? resolved.
```

Ln 114
```
//? dj: The terminology of client/server (e.g. `ServerMethod`/`ClientMethod`) is very easy to mix up.
//? What do you think of `IncomingMethod`/`OutgoingMethod`? (From the perspective of our CDC service.)
//? batiati: I don't want to introduce a new terminology, as it follows the spec defined by the
//? "chassis" tag.
//? resolved.
```
## File docs/operating/cdc.md

(Not sure how to add review comments to documentation...)

- Instead of the `curl` command for downloading TigerBeetle I think we should instead specify that you should use the same TigerBeetle binary as your cluster is deployed with.
//? resolved.