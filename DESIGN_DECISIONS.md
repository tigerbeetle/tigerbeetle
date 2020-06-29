While we could run validations before journalling and replicating transfers,
and fail a transfer early if there is anything wrong with it, this would
bifurcate the error return path and complicate how we return errors for a batch
of transfers. We therefore leave all error handling logic to the business logic.
This reduces the number of branches in the code and keeps things simple.
