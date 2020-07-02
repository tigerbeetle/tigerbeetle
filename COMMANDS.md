# Commands

**CREATE_PARTICIPANTS**: Create a batch of participants if they do not exist.

**UPDATE_PARTICIPANTS**: Update a batch of participants only if they exist and only if each participant's current state uid matches the previous state uid in the struct provided by the caller.

**CREATE_TRANSFERS**: Create a batch of transfers (maps to prepares in Mojaloop).
**ACCEPT_TRANSFERS**: Accept a batch of transfers (maps to fulfils in Mojaloop).
**REJECT_TRANSFERS**: Reject a batch of transfers (could not be fulfilled for some reason).

**UPDATE_TRANSFERS**: Update a batch of transfers (e.g. for admin purposes) only if they exist and only if each transfer's current state uid (vector_1) matches the previous state uid (vector_0) in the struct provided by the caller.

This will expose compare-and-swap powers and enable optimistic transactions on individual participants and transfers right out the gate, without anything complicated. This chaining of entropy will also enable interactions with TigerBeetle to be safer by exposing any race conditions in user code, so that a user can only update a value if its previous value is what they expect it to be.

The command is specified once per batch, on the outside of the batch, not per item within the batch, so that we don't have conditional branching in the critical path, to make everything predictable for the CPU and keep the data hot without thrashing the L1 cache.

For **GET commands** (participants, transfers), we will need some kind of query power, e.g. GET accepted transfers for a given participant since a certain timestamp etc.

For a batch, a response will explicitly list the ids and error codes of failed structs, everything else succeeded, so the happy path is better than O(N) in the number of transfers.
