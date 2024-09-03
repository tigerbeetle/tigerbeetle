import java.math.BigInteger;

public abstract class Command {
  abstract String pretty();
}


class CreateAccounts extends Command {
  NewAccount[] accounts;

  @Override
  String pretty() {
    return "Creating %d accounts".formatted(accounts.length);
  }
}


class CreateTransfers extends Command {
  NewTransfer[] transfers;

  @Override
  String pretty() {
    return "Creating %d transfers".formatted(transfers.length);
  }
}


class LookupAccounts extends Command {
  long[] ids;

  @Override
  String pretty() {
    return "Looking up %d accounts".formatted(ids.length);
  }
}


class NewAccount {
  long id;
  int ledger;
  int code;
  int flags;
}


class NewTransfer {
  long id;
  long debitAccountId;
  long creditAccountId;
  int ledger;
  int code;
  BigInteger amount;
  int flags;
}
