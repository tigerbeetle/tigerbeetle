# Tiger Style

## The essence of style

> “There are three things extremely hard: steel, a diamond, and to know one's self.” — Benjamin Franklin

TigerBeetle's coding style is evolving. A collective give-and-take at the intersection of engineering and art. Numbers and human intuition. Reason and experience. First principles and knowledge. Precision and poetry. Just like music. A tight beat. A rare groove. Words that rhyme and rhymes that break. Biodigital jazz. This is what we've learned along the way. The best is yet to come.

## Why have style?

Another word for style is design.

> “The design is not just what it looks like and feels like. The design is how it works.” — Steve Jobs

Our design goals are safety, performance, and developer experience. In that order. All three are important. Good style advances these goals. Does the code make for more or less safety, performance or developer experience? That is why we need style.

Put this way, style is more than readability, and readability is table stakes, a means to an end rather than an end in itself.

> “...in programming, style is not something to pursue directly. Style is necessary only where understanding is missing.” ─ [Let Over Lambda](https://letoverlambda.com/index.cl/guest/chap1.html)

This document explores how we apply these design goals to coding style. First, a word on simplicity, elegance and technical debt.

## On simplicity and elegance

Simplicity is not a free pass. It's not in conflict with our design goals. It need not be a concession or a compromise.

Rather, simplicity is how we bring our design goals together, how we identify the “super idea” that solves the axes simultaneously, to achieve something elegant.

> “Simplicity and elegance are unpopular because they require hard work and discipline to achieve” — Edsger Dijkstra

Contrary to popular belief, simplicity is also not the first attempt but the hardest revision. It's easy to say “let's do something simple”, but to do that in practice takes thought, multiple passes, many sketches, and still we may have to [“throw one away”](https://en.wikipedia.org/wiki/The_Mythical_Man-Month).

The hardest part, then, is how much thought goes into everything.

We spend this mental energy upfront, proactively rather than reactively, because we know that when the thinking is done, what is spent on the design will be dwarfed by the implementation and testing, and then again by the costs of operation and maintenance.

An hour or day of design is worth weeks or months in production:

> “the simple and elegant systems tend to be easier and faster to design and get right, more efficient in execution, and much more reliable” — Edsger Dijkstra

## Technical debt

What could go wrong? What's wrong? Which question would we rather ask? The former, because code, like steel, is less expensive to change while it's hot. A problem solved in production is many times more expensive than a problem solved in implementation, or a problem solved in design.

Since it's hard enough to discover showstoppers, when we do find them, we solve them. We don't allow potential memcpy latency spikes, or exponential complexity algorithms to slip through.

> “You shall not pass!” — Gandalf

In other words, TigerBeetle has a “zero technical debt” policy. We do it right the first time. This is important because the second time may not transpire, and because doing good work, that we can be proud of, builds momentum.

We know that what we ship is solid. We may lack crucial features, but what we have meets our design goals. This is the only way to make steady incremental progress, knowing that the progress we have made is indeed progress.

## Safety

> “The rules act like the seat-belt in your car: initially they are perhaps a little uncomfortable, but after a while their use becomes second-nature and not using them becomes unimaginable.” — Gerard J. Holzmann

[NASA's Power of Ten — Rules for Developing Safety Critical Code](https://spinroot.com/gerard/pdf/P10.pdf) will change the way you code forever. To expand:

* Use **only very simple, explicit control flow** for clarity. **Do not use recursion** to ensure that all executions that should be bounded are bounded. Use **only a minimum of excellent abstractions** but only if they make the best sense of the domain. Abstractions are [never zero cost](https://isaacfreund.com/blog/2022-05/). Every abstraction introduces the risk of a leaky abstraction.

* **Put a limit on everything** because, in reality, this is what we expect—everything has a limit. For example, all loops and all queues must have a fixed upper bound to prevent infinite loops or tail latency spikes. This follows the [“fail-fast”](https://en.wikipedia.org/wiki/Fail-fast) principle so that violations are detected sooner rather than later. Where a loop cannot terminate (e.g. an event loop), this must be asserted.

* **Assertions detect programmer errors. Unlike operating errors, which are expected and which must be handled, assertion failures are unexpected. The only correct way to handle corrupt code is to crash. Assertions downgrade catastrophic correctness bugs into liveness bugs. Assertions are a force multiplier for discovering bugs by fuzzing.**

  * **Assert all function arguments and return values, pre/postconditions and invariants.** A function must not operate blindly on data it has not checked. The purpose of a function is to increase the probability that a program is correct. Assertions within a function are part of how functions serve this purpose. The assertion density of the code must average a minimum of two assertions per function.

  * On occasion, you may use a blatantly true assertion instead of a comment as stronger documentation where the assertion condition is critical and surprising.

  * **Assert the relationships of compile-time constants** as a sanity check, and also to document and enforce [subtle invariants](https://github.com/coilhq/tigerbeetle/blob/db789acfb93584e5cb9f331f9d6092ef90b53ea6/src/vsr/journal.zig#L45-L47) or [type sizes](https://github.com/coilhq/tigerbeetle/blob/578ac603326e1d3d33532701cb9285d5d2532fe7/src/ewah.zig#L41-L53). Compile-time assertions are extremely powerful because they are able to check a program's design integrity *before* the program even executes.

  * **The golden rule of assertions is to assert the _positive space_ that you do expect AND to assert the _negative space_ that you do not expect** because where data moves across the valid/invalid boundary between these spaces is where interesting bugs are often found. This is also why **tests must test exhaustively**, not only with valid data but also with invalid data, and as valid data becomes invalid.

* All memory must be statically allocated at startup. **No memory may be dynamically allocated (or freed and reallocated) after initialization.** This avoids unpredictable behavior that can significantly affect performance, and avoids use-after-free. As a second-order effect, it is our experience that this also makes for more efficient, simpler designs that are more performant and easier to maintain and reason about, compared to designs that do not consider all possible memory usage patterns upfront as part of the design.

* Declare variables at the **smallest possible scope**, and **minimize the number of variables in scope**, to reduce the probability that variables are misused.

* Restrict the length of function bodies to reduce the probability of poorly structured code. We enforce a **hard limit of 70 lines per function**.

* Appreciate, from day one, **all compiler warnings at the compiler's strictest setting**.

Beyond these rules:

* Compound conditions that evaluate multiple booleans make it difficult for the reader to verify that all cases are handled. Split compound conditions into simple conditions using nested `if/else` branches. Split complex `else if` chains into `else { if { } }` trees. This makes the branches and cases clear. Again, consider whether a single `if` does not also need a matching `else` branch, to ensure that the positive and negative spaces are handled or asserted.

* All errors must be handled. An [analysis of production failures in distributed data-intensive systems](https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-yuan.pdf) found that the majority of catastrophic failures could have been prevented by simple testing of error handling code.

> “Specifically, we found that almost all (92%) of the catastrophic system failures are the result of incorrect handling of non-fatal errors explicitly signaled in software.”

* **Always motivate, always say why**. Never forget to say why. Because if you explain the rationale for a decision, it not only increases the hearer's understanding, and makes them more likely to adhere or comply, but it also shares criteria with them with which to evaluate the decision and its importance.

## Performance

> “The lack of back-of-the-envelope performance sketches is the root of all evil.” — Rivacindela Hudsoni

* Think about performance from the outset, from the beginning. **The best time to solve performance, to get the huge 1000x wins, is in the design phase, which is precisely when we can't measure or profile.** It's also typically harder to fix a system after implementation and profiling, and the gains are less. So you have to have mechanical sympathy. Like a carpenter, work with the grain.

* **Perform back-of-the-envelope sketches with respect to the four resources (network, disk, memory, CPU) and their two main characteristics (bandwidth, latency).** Sketches are cheap. Use sketches to be “roughly right” and land within 90% of the global maximum.

* Optimize for the slowest resources first (network, disk, memory, CPU) in that order, after compensating for the frequency of usage, because faster resources may be used many times more. For example, a memory cache miss may be as expensive as a disk fsync, if it happens many times more.

* Distinguish between the control plane and data plane. A clear delineation between control plane and data plane through the use of batching enables a high level of assertion safety without losing performance. See our [July 2021 talk on Zig SHOWTIME](https://youtu.be/BH2jvJ74npM?t=1958) for examples.

* Amortize network, disk, memory and CPU costs by batching accesses.

* Let the CPU be a sprinter doing the 100m. Be predictable. Don't force the CPU to zig zag and change lanes. Give the CPU large enough chunks of work. This comes back to batching.

* Be explicit. Minimize dependence on the compiler to do the right thing for you.

## Developer experience

> “There are only two hard things in Computer Science: cache invalidation, naming things, and off-by-one errors.” — Phil Karlton

### Naming things

* **Get the nouns and verbs just right.** Great names are the essence of great code, they capture what a thing is or does, and provide a crisp, intuitive mental model. They show that you understand the domain. Take time to find the perfect name, to find nouns and verbs that work together, so that the whole is greater than the sum of its parts.

* Use `snake_case` for function and variable names. The underscore is the closest thing we have as programmers to a space, and helps to separate words and encourage descriptive names.

* Follow the Zig style guide.

* Do not abbreviate variable names, unless the variable is a primitive integer type used as an argument to a sort function or matrix calculation.

* Add units or qualifiers to variable names, and put the units or qualifiers last, sorted by descending significance, so that the variable starts with the most significant word, and ends with the least significant word. For example, `latency_ms_max` rather than `max_latency_ms`. This will then line up nicely when `latency_ms_min` is added, as well as group all variables that relate to latency.

* When choosing related names, try hard to find names with the same number of characters so that related variables all line up in the source. For example, as arguments to a memcpy function, `source` and `target` are better than `src` and `dest` because they have the second-order effect that any related variables such as `source_offset` and `target_offset` will all line up in calculations and slices. This makes the code symmetrical, with clean blocks that are easier for the eye to parse and for the reader to check.

* When a single function calls out to a helper function or callback, prefix the name of the helper function with the name of the calling function to show the call history. For example, `read_sector()` and `read_sector_callback()`.

* Don't overload names with multiple meanings that are context-dependent. For example, TigerBeetle has a feature called *pending transfers* where a pending transfer can be subsequently *posted* or *voided*. At first, we called them *two-phase commit transfers*, but this overloaded the *two-phase commit* terminology that was used in our consensus protocol, causing confusion.

* Think of how names will be used outside the code, in documentation or communication. For example, a noun is often a better descriptor than an adjective or present participle, because a noun can be directly used in correspondence without having to be rephrased. Compare `replica.pipeline` vs `replica.preparing`. The former can be used directly as a section header in a document or conversation, whereas the latter must be clarified. Noun names compose more clearly for derived identifiers, e.g. `config.pipeline_max`.

* **Write descriptive commit messages** that inform and delight the reader, because your commit messages are being read.

* Don't forget to say why. Code alone is not documentation. Use comments to explain why you wrote the code the way you did. Show your workings.

* Don't forget to say how. For example, when writing a test, think of writing a description at the top to explain the goal and methodology of the test, to help your reader get up to speed, or to skip over sections, without forcing them to dive in.

* Comments are sentences, with a space after the slash, with a capital letter and a full stop, or a colon if they relate to something that follows. Comments after the end of a line can be phrases, with no punctuation.

### Cache invalidation

* Don't duplicate variables or take aliases to them. This will reduce the probability that state gets out of sync.

* If you don't mean a function argument to be copied when passed by value, and if the argument type is more than 16 bytes, then pass the argument as `*const`. This will catch bugs where the caller makes an accidental copy on the stack before calling the function.

* **Shrink the scope** to minimize the number of variables at play and reduce the probability that the wrong variable is used.

* Calculate or check variables close to where/when they are used. **Don't introduce variables before they are needed.** Don't leave them around where they are not. This will reduce the probability of a POCPOU (place-of-check to place-of-use), a distant cousin to the infamous [TOCTOU](https://en.wikipedia.org/wiki/Time-of-check_to_time-of-use). Most bugs come down to a semantic gap, caused by a gap in time or space, because it's harder to check code that's not contained along those dimensions.

* Use simpler function signatures and return types to reduce dimensionality at the call site, the number of branches that need to be handled at the call site, because this dimensionality can also be viral, propagating through the call chain. For example, as a return type, `void` trumps `bool`, `bool` trumps `u64`, `u64` trumps `?u64`, and `?u64` trumps `!u64`.

* Ensure that functions run to completion without suspending, so that precondition assertions are true throughout the lifetime of the function. These assertions are useful documentation without a suspend, but may be misleading otherwise.

* Be on your guard for **[buffer bleeds](https://en.wikipedia.org/wiki/Heartbleed)**. This is a buffer underflow, the opposite of a buffer overflow, where a buffer is not fully utilized, with padding not zeroed correctly. This may not only leak sensitive information, but may cause deterministic guarantees as required by TigerBeetle to be violated.

* Use newlines to **group resource allocation and deallocation**, i.e. before the resource allocation and after the corresponding `defer` statement, to make leaks easier to spot.

* TigerBeetle has **a “zero dependencies” policy**, apart from the Zig toolchain. Dependencies, in general, inevitably lead to supply chain attacks, safety and performance risk, and slow install times. For foundational infrastructure in particular, the cost of any dependency is further amplified throughout the rest of the stack.

### Off-by-one errors

* **The usual suspects for off-by-one errors are casual interactions between an `index`, a `count` or a `size`.** These are all primitive integer types, but should be seen as distinct types, with clear rules to cast between them. To go from an `index` to a `count` you need to add one, since indexes are *0-based* but counts are *1-based*. To go from a `count` to a `size` you need to multiply by the unit. Again, this is why including units and qualifiers in variable names is important.

* Show your intent with respect to division. For example, use `@divExact()`, `@divFloor()` or `div_ceil()` to show the reader you've thought through all the interesting scenarios where rounding may be involved.

### Style by the numbers

* Run `zig fmt`.

* Use 4 spaces of indentation, rather than 2 spaces, as that is more obvious to the eye at a distance.

* Hard limit all line lengths, without exception, to at most 100 columns for a good typographic "measure". Use it up. Never go beyond. Nothing should be hidden by a horizontal scrollbar. Let your editor help you by setting a column ruler. To wrap a function signature, call or data structure, add a trailing comma, close your eyes and let `zig fmt` do the rest.

## The Last Stage

At the end of the day, keep trying things out, have fun, and remember—it's called TigerBeetle, not only because it's fast, but because it's small!

> You don’t really suppose, do you, that all your adventures and escapes were managed by mere luck, just for your sole benefit? You are a very fine person, Mr. Baggins, and I am very fond of you; but you are only quite a little fellow in a wide world after all!”
> 
> “Thank goodness!” said Bilbo laughing, and handed him the tobacco-jar.
