# Mojaloop - Central Services
Business logic operations for Mojaloop `central-ledger` and `central-settlement`.

## Central-Ledger
MJL service responsible for clearing.

### Prepare-Position
Phase-1/2 of the 2-phase transfer.

Prepare duplicate check:
```sql
--1587309924652
INSERT INTO `transferDuplicateCheck` (`hash`, `transferId`) VALUES (?, ?)
```

Validations:
```javascript
validateFspiopSourceMatchesPayer(payload, headers) // asserts headers['fspiop-source'] matches payload.payerFsp
validateParticipantByName(payload.payerFsp) // asserts payer participant exists by doing a lookup by name (discards result of lookup work)
validatePositionAccountByNameAndCurrency(payload.payerFsp, payload.amount.currency) // asserts account exists for name-currency tuple (discards result of lookup work)
validateParticipantByName(payload.payeeFsp) // asserts payee participant exists by doing a lookup by name (discards result of lookup work)
validatePositionAccountByNameAndCurrency(payload.payeeFsp, payload.amount.currency) // asserts account exists for name-currency tuple (discards result of lookup work)
validateAmount(payload.amount) // Validates allowed scale of decimal places and allowed precision
validateConditionAndExpiration(payload) // Validates condition and expiration
  assert(payload.condition)
  assert(FiveBellsCondition.validateCondition('ni:///sha-256;' + payload.condition + '?fpt=preimage-sha-256&cost=0'))
  assert(payload.expiration)
  assert(Date.parse(payload.expiration) >= Date.parse(new Date().toDateString()))
validateDifferentDfsp(payload) // asserts lowercase string comparison of payload.payerFsp and payload.payeeFsp is different

saveTransferPreparedChangePosition()
```

Write the transfer to the DB...AND attempt payer dfsp position adjustment at the same time:
```sql

--1587309924656
INSERT INTO `transfer` (`amount`, `currencyId`, `expirationDate`, `ilpCondition`, `transferId`) VALUES (?, ?, ?, ?, ?)
--1587309924656 
INSERT INTO `transferParticipant` (`amount`, `ledgerEntryTypeId`, `participantCurrencyId`, `transferId`, `transferParticipantRoleTypeId`) VALUES (?, ?, ?, ?, ?)
--1587309924656
INSERT INTO `transferParticipant` (`amount`, `ledgerEntryTypeId`, `participantCurrencyId`, `transferId`, `transferParticipantRoleTypeId`) VALUES (?, ?, ?, ?, ?)
--1587309924656 (Inserted as part of a batch)
INSERT INTO `transferExtension` (`transferId`, `key`, `value`) VALUES (?, ?, ?)
--1587309924656
INSERT INTO `ilpPacket` (`transferId`, `value`) VALUES (?, ?)
--1587309924658
INSERT INTO `transferStateChange` (`createdDate`, `reason`, `transferId`, `transferStateId`) VALUES (?, ?, ?, ?)
--1587309924660 
UPDATE participantPosition SET value = (value + 100), changedDate = '2020-04-19 15:25:24.655' WHERE participantPositionId = 3 AND (value + 100) < (SELECT value FROM participantLimit WHERE participantLimitId = 1)
--1587309924661 
INSERT INTO participantPositionChange (participantPositionId, transferStateChangeId, value, reservedValue, createdDate) SELECT 3, 1253, value, reservedValue, '2020-04-19 15:25:24.655' FROM participantPosition WHERE participantPositionId = 3
```

```javascript
const names = [payload.payeeFsp, payload.payerFsp]
const participants = [] // filled out by doing lookups for both participants in names array

const insert_transfer = {
  amount: payload.amount.amount,
  currencyId: payload.amount.currency,
  expirationDate: Time.getUTCString(new Date(payload.expiration)),
  ilpCondition: payload.condition,
  transferId: payload.transferId
}

const payerTransferParticipantRecord = {
  amount: payload.amount.amount,
  ledgerEntryTypeId: Enum.Accounts.LedgerEntryType.PRINCIPLE_VALUE,
  participantCurrencyId: participantCurrencyIds[payload.payerFsp],
  transferId: payload.transferId,
  transferParticipantRoleTypeId: Enum.Accounts.TransferParticipantRoleType.PAYER_DFSP
}

const payeeTransferParticipantRecord = {
  amount: -payload.amount.amount,
  ledgerEntryTypeId: Enum.Accounts.LedgerEntryType.PRINCIPLE_VALUE,
  participantCurrencyId: participantCurrencyIds[payload.payeeFsp],
  transferId: payload.transferId,
  transferParticipantRoleTypeId: Enum.Accounts.TransferParticipantRoleType.PAYEE_DFSP
}

const insert_ilpPacket = {
  transferId: payload.transferId,
  value: payload.ilpPacket
}

const transferStateChangeRecord = {
  createdDate: Time.getUTCString(now),
  reason: stateReason = null,
  transferId: payload.transferId,
  transferStateId: Enum.Transfers.TransferInternalState.RESERVED
}

let transferExtensionsRecordList = []
if (payload.extensionList && payload.extensionList.extension) {
  transferExtensionsRecordList = payload.extensionList.extension.map(ext => {
    return {
      transferId: payload.transferId,
      key: ext.key,
      value: ext.value
    }
  })
  await knex.batchInsert('transferExtension', transferExtensionsRecordList).transacting(trx)
}
```

Final:
Send notification to payee DFSP.

### Fulfil-Position
Phase-2/2 of the 2-phase transfer.

Write the transfer fulfillment to the DB:
```sql
--1587309924755 - TransferService.getById(transferId) 
SELECT `transfer`.*, `transfer`.`currencyId` AS `currency`, `pc1`.`participantCurrencyId` AS `payerParticipantCurrencyId`, `tp1`.`amount` AS `payerAmount`, `da`.`participantId` AS `payerParticipantId`, `da`.`name` AS `payerFsp`, `pc2`.`participantCurrencyId` AS `payeeParticipantCurrencyId`, `tp2`.`amount` AS `payeeAmount`, `ca`.`participantId` AS `payeeParticipantId`, `ca`.`name` AS `payeeFsp`, `tsc`.`transferStateChangeId`, `tsc`.`transferStateId` AS `transferState`, `tsc`.`reason` AS `reason`, `tsc`.`createdDate` AS `completedTimestamp`, `ts`.`enumeration` AS `transferStateEnumeration`, `ts`.`description` AS `transferStateDescription`, `ilpp`.`value` AS `ilpPacket`, `transfer`.`ilpCondition` AS `condition`, `tf`.`ilpFulfilment` AS `fulfilment`, `te`.`errorCode`, `te`.`errorDescription` 
FROM `transfer`
INNER JOIN `transferParticipant` AS `tp1` ON `tp1`.`transferId` = `transfer`.`transferId`
INNER JOIN `transferParticipantRoleType` AS `tprt1` ON `tprt1`.`transferParticipantRoleTypeId` = `tp1`.`transferParticipantRoleTypeId` 
INNER JOIN `participantCurrency` AS `pc1` ON `pc1`.`participantCurrencyId` = `tp1`.`participantCurrencyId` 
INNER JOIN `participant` AS `da` ON `da`.`participantId` = `pc1`.`participantId` 
INNER JOIN `transferParticipant` AS `tp2` ON `tp2`.`transferId` = `transfer`.`transferId` 
INNER JOIN `transferParticipantRoleType` AS `tprt2` ON `tprt2`.`transferParticipantRoleTypeId` = `tp2`.`transferParticipantRoleTypeId` 
INNER JOIN `participantCurrency` AS `pc2` ON `pc2`.`participantCurrencyId` = `tp2`.`participantCurrencyId` 
INNER JOIN `participant` AS `ca` ON `ca`.`participantId` = `pc2`.`participantId`
INNER JOIN `ilpPacket` AS `ilpp` ON `ilpp`.`transferId` = `transfer`.`transferId` 
LEFT JOIN `transferStateChange` AS `tsc` ON `tsc`.`transferId` = `transfer`.`transferId` 
LEFT JOIN `transferState` AS `ts` ON `ts`.`transferStateId` = `tsc`.`transferStateId` 
LEFT JOIN `transferFulfilment` AS `tf` ON `tf`.`transferId` = `transfer`.`transferId` 
LEFT JOIN `transferError` AS `te` ON `te`.`transferId` = `transfer`.`transferId` 
WHERE `transfer`.`transferId` = ?  AND `tprt1`.`name` = ?  AND `tprt2`.`name` = ?  AND pc1.currencyId = transfer.currencyId  AND pc2.currencyId = transfer.currencyId 
ORDER BY `tsc`.`transferStateChangeId` desc limit ?
--1587309924757 
SELECT * FROM `transferExtension` WHERE `transferId` = ?  AND `isFulfilment` = ?  AND `isError` = ?
--1587309924759 
INSERT INTO `transferFulfilmentDuplicateCheck` (`hash`, `transferId`) VALUES (?, ?)
--1587309924768 
SELECT `transferParticipant`.*, `tsc`.`transferStateId`, `tsc`.`reason` FROM `transferParticipant` INNSER JOIN `transferStateChange` AS `tsc` ON `tsc`.`transferId` = `transferParticipant`.`transferId` WHERE `transferParticipant`.`transferId` = ? and `transferParticipant`.`transferParticipantRoleTypeId` = ?  AND `transferParticipant`.`ledgerEntryTypeId` = ? ORDER BY `tsc`.`transferStateChangeId` desc limit ?
--1587309924770 
SELECT `settlementWindow`.`settlementWindowId`, `swsc`.`settlementWindowStateId` AS `state`, `swsc`.`reason` AS `reason`, `settlementWindow`.`createdDate` AS `createdDate`, `swsc`.`createdDate` AS `changedDate` 
FROM `settlementWindow` LEFT JOIN `settlementWindowStateChange` AS `swsc` ON `swsc`.`settlementWindowStateChangeId` = `settlementWindow`.`currentStateChangeId` WHERE `swsc`.`settlementWindowStateId` = ? ORDER BY `changedDate` desc
--1587309924771 
INSERT INTO `transferFulfilment` (`completedDate`, `createdDate`, `ilpFulfilment`, `isValid`, `settlementWindowId`, `transferId`) VALUES (?, ?, ?, ?, ?, ?)
--1587309924774 
INSERT INTO `transferStateChange` (`createdDate`, `transferId`, `transferStateId`) VALUES (?, ?, ?)
--1587309924775 
UPDATE participantPosition SET value = (value + -100), changedDate = '2020-04-19 15:25:24.769' WHERE participantPositionId = 5
--1587309924775 
SELECT * FROM `transferStateChange` WHERE `transferId` = ? ORDER BY `transferStateChangeId` desc limit ? for update
--1587309924776 
INSERT INTO participantPositionChange (participantPositionId, transferStateChangeId, value, reservedValue, createdDate) SELECT 5, 1254, value, reservedValue, '2020-04-19 15:25:24.769' FROM participantPosition WHERE participantPositionId = 5
```

Validate:
```javascript
if (payload.fulfilment && !Validator.validateFulfilCondition(payload.fulfilment, transfer.condition));
if (transfer.transferState !== TransferState.RESERVED);
if (transfer.expirationDate <= new Date(Util.Time.getUTCString(new Date())));
```

```javascript
await TransferService.handleResponseAdjustPosition(transferId, payload, action)
await TransferFacade.fulfilPosition

const transferStateChangePosition = {
  transferId: transferId,
  transferStateId: Enum.Transfers.TransferState.COMMITTED
}
const { participantCurrencyId, amount } = await getTransferInfoToChangePosition(transferId, Enum.Accounts.TransferParticipantRoleType.PAYEE_DFSP, Enum.Accounts.LedgerEntryType.PRINCIPLE_VALUE)

const transferFulfilmentRecord = {
  transferId,
  ilpFulfilment: payload.fulfilment || null,
  completedDate: completedTimestamp,
  isValid: !fspiopError,
  settlementWindowId: null,
  createdDate: transactionTimestamp
}

await calculateFulfilPositionRawQuery(participantCurrencyId, amount, transactionTimestamp, insertedTransferStateChange, payload, trx)

const participantPosition = await knex('participantPosition').transacting(trx).where({ participantCurrencyId }).forUpdate().select('*').first()
let latestPosition
if (isReversal) {
  latestPosition = new MLNumber(participantPosition.value).subtract(amount)
} else {
  latestPosition = new MLNumber(participantPosition.value).add(amount)
}
latestPosition = latestPosition.toFixed(Config.AMOUNT.SCALE)
await knex('participantPosition').transacting(trx).where({ participantCurrencyId }).update({
  value: latestPosition,
  changedDate: transactionTimestamp
})

const participantPositionChange = {
  participantPositionId: participantPosition.participantPositionId,
  transferStateChangeId: insertedTransferStateChange.transferStateChangeId,
  value: latestPosition,
  reservedValue: participantPosition.reservedValue,
  createdDate: transactionTimestamp
}
```

## Central-Settlement
MJL service responsible for settlement.

### Settlement Event Trigger
Initiate the settlement for all applicable settlement models via function `settlementEventTrigger`.

```javascript
const { settlementModel, reason, settlementWindows } = params
const settlementModelData = await SettlementModelModel.getByName(settlementModel)

if (settlementModelData.settlementGranularityId === enums.settlementGranularity.GROSS ||
      settlementModelData.settlementDelayId === enums.settlementDelay.IMMEDIATE) {
      const error = ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'Settlement can not be created for GROSS or IMMEDIATE models')

// Retrieve windows by active models:
  getByListOfIds: async function (listOfIds, settlementModel, winStateEnum) {
    const knex = await Db.getKnex()
    return Db.from('settlementWindow').query(builder => {
      const b = builder
        .join('settlementWindowStateChange AS swsc', 'swsc.settlementWindowStateChangeId', 'settlementWindow.currentStateChangeId')
        .join('settlementWindowContent AS swc', 'swc.settlementWindowId', 'settlementWindow.settlementWindowId')
        .join('settlementWindowContentStateChange AS swcsc', 'swcsc.settlementWindowContentStateChangeId', 'swc.currentStateChangeId')
        .whereRaw(`settlementWindow.settlementWindowId IN (${listOfIds})`)
        .where('swc.ledgerAccountTypeId', settlementModel.ledgerAccountTypeId)
        .where('swc.currencyId', knex.raw('COALESCE(?, swc.currencyId)', settlementModel.currencyId))
        .whereIn('swsc.settlementWindowStateId', [winStateEnum.CLOSED, winStateEnum.ABORTED, winStateEnum.PENDING_SETTLEMENT])
        .whereIn('swcsc.settlementWindowStateId', [winStateEnum.CLOSED, winStateEnum.ABORTED])
        .distinct(
          'settlementWindow.settlementWindowId',
          'swsc.settlementWindowStateId as state'
        )
      return b
    })
  },
// settlement event trigger
const settlementId = await SettlementModel.triggerSettlementEvent({ idList, reason }, settlementModelData, enums)
// retrieve resulting data for response
const settlement = await SettlementModel.getById({ settlementId })
```

Obtain all models for settlements:
```sql
--1587309924788  - Get settlement model by name:
SELECT * FROM `settlementModel` WHERE `name` = ? AND isActive = 1
--1587309924789  - Get all settlement windows by settlement models:
SELECT * FROM `settlementWindow` 
LEFT JOIN settlementWindowStateChange AS swsc ON swsc.settlementWindowStateChangeId = settlementWindow.currentStateChangeId 
LEFT JOIN settlementWindowContent AS swc ON swc.settlementWindowId = settlementWindow.settlementWindowId 
LEFT JOIN settlementWindowContentStateChange AS swcsc ON swcsc.settlementWindowContentStateChangeId = swc.currentStateChangeId 
WHERE settlementWindow.settlementWindowId IN ? 
and ledgerAccountTypeId = ? 
and swc.currencyId = ?
and swsc.settlementWindowStateId = winStateEnum.CLOSED, winStateEnum.ABORTED, winStateEnum.PENDING_SETTLEMENT
and swcsc.settlementWindowStateId = [winStateEnum.CLOSED, winStateEnum.ABORTED]
```

Create the settlement for each model:
```javascript
 const settlementId = await knex('settlement').transacting(trx)
  .insert({
    reason,
    createdDate: transactionTimestamp,
    settlementModelId: settlementModel.settlementModelId
  })
  const settlementSettlementWindowList = idList.map(settlementWindowId => {
  return {
    settlementId,
    settlementWindowId,
    createdDate: transactionTimestamp
  }
})
// associate settlement windows with the settlement
await knex.batchInsert('settlementSettlementWindow', settlementSettlementWindowList).transacting(trx)
 // retrieve affected settlementWindowContent
let swcList = await knex('settlementWindow AS sw').transacting(trx)
  .join('settlementWindowStateChange AS swsc', 'swsc.settlementWindowStateChangeId', 'sw.currentStateChangeId')
  .join('settlementWindowContent AS swc', 'swc.settlementWindowId', 'sw.settlementWindowId')
  .join('settlementWindowContentStateChange AS swcsc', 'swcsc.settlementWindowContentStateChangeId', 'swc.currentStateChangeId')
  .whereRaw(`sw.settlementWindowId IN (${idList})`)
  .where('swc.ledgerAccountTypeId', settlementModel.ledgerAccountTypeId)
  .where('swc.currencyId', knex.raw('COALESCE(?, swc.currencyId)', settlementModel.currencyId))
  .whereIn('swsc.settlementWindowStateId', [enums.settlementWindowStates.CLOSED, enums.settlementWindowStates.ABORTED, enums.settlementWindowStates.PENDING_SETTLEMENT])
  .whereIn('swcsc.settlementWindowStateId', [enums.settlementWindowStates.CLOSED, enums.settlementWindowStates.ABORTED])
```
```sql
--1587309924790  - Insert a settlement for each modelId.
INSERT INTO `settlement` (reason, createdDate, settlementModelId) VALUES (?, ?, ?)
--1587309924791 - Associate settlement windows with the settlement
INSERT INTO `settlementSettlementWindow` (settlementId, settlementWindowId, createdDate) VALUES (?, ?, ?)
--1587309924792 - Retrieve affected settlementWindowContent
SELECT * FROM `settlementWindow AS sw` 
LEFT JOIN settlementWindowStateChange AS swsc ON swsc.settlementWindowStateChangeId = settlementWindow.currentStateChangeId 
LEFT JOIN settlementWindowContent AS swc ON swc.settlementWindowId = settlementWindow.settlementWindowId 
LEFT JOIN settlementWindowContentStateChange AS swcsc ON swcsc.settlementWindowContentStateChangeId = swc.currentStateChangeId 
WHERE settlementWindow.settlementWindowId IN ? 
AND ledgerAccountTypeId = ? 
AND swc.currencyId = ?
AND swsc.settlementWindowStateId = winStateEnum.CLOSED, winStateEnum.ABORTED, winStateEnum.PENDING_SETTLEMENT
AND swcsc.settlementWindowStateId = [winStateEnum.CLOSED, winStateEnum.ABORTED]
--1587309924793 - Bind requested settlementWindowContent AND settlementContentAggregation records
UPDATE `settlementWindowContent` SET settlementId = ? WHERE settlementWindowContentId = ?
--1587309924794
UPDATE `settlementContentAggregation` SET currentStateId = `enums.settlementWindowStates.PENDING_SETTLEMENT` WHERE `settlementWindowContentId` IN ?
--1587309924795 - Update state
INSERT INTO `settlementWindowContentStateChange` (settlementWindowContentId, settlementWindowStateId, reason, createdDate) 
VALUES (?, enums.settlementStates.PENDING_SETTLEMENT, ?, ?)  
--1587309924796 - Update the current state for settlement window content
UPDATE `settlementWindowContent` SET currentStateChangeId = ? WHERE settlementWindowContentId = ?
--1587309924797 - Create the settlement accounts
INSERT INTO `settlementParticipantCurrency` (settlementId, participantCurrencyId, createdDate, netAmount)
VALUES SELECT sca.settlementId,sca.participantCurrencyId,?,sca.amount FROM settlementContentAggregation AS sca WHERE sca.settlementId = ?
--1587309924798 - Change settlementParticipantCurrency records state
SELECT settlementParticipantCurrencyId FROM `settlementParticipantCurrency` WHERE settlementId = ?
--1587309924799
INSERT INTO `settlementParticipantCurrencyStateChange` (settlementParticipantCurrencyId, settlementStateId, reason, createdDate) 
VALUES (?, enums.settlementStates.PENDING_SETTLEMENT, ?, ?)
--1587309924800 - Update the pc to point to the new state
UPDATE `settlementParticipantCurrency` SET currentStateChangeId = ? WHERE settlementParticipantCurrencyId = ?
--1587309924801 - Set state of CLOSED AND ABORTED windows to PENDING_SETTLEMENT, skip already in PENDING_SETTLEMENT state
SELECT sw.settlementWindowId FROM settlementWindow AS sw 
LEFT JOIN settlementWindowStateChange AS swsc ON swsc.settlementWindowStateChangeId = sw.currentStateChangeId
WHERE sw.settlementWindowId = ? AND swsc.settlementWindowStateId IN (enums.settlementWindowStates.CLOSED, enums.settlementWindowStates.ABORTED)
--1587309924802
INSERT INTO `settlementWindowStateChange` (settlementWindowId, settlementWindowStateId, reason, createdDate) VALUES
(?, enums.settlementStates.PENDING_SETTLEMENT, ?, ?)
--1587309924803
UPDATE `settlementWindow` SET `currentStateChangeId` = ? WHERE settlementWindowId = ? 
--1587309924804 - Initiate settlement state to PENDING_SETTLEMENT
INSERT INTO `settlementStateChange` (settlementId, settlementStateId, reason, createdDate) 
VALUES (?, enums.settlementStates.PENDING_SETTLEMENT, ?, ?) 
--1587309924805
UPDATE `settlement` SET settlementStateId = ? WHERE settlementId = ?
```

### Settlement Events
The `updateSettlementById` endpoint is used repeatedly to manage the settlement process. 
The current settlement state drive what type of processing should occur next.
Example: if the current state is `PENDING_SETTLEMENT`, the next processing event to take place would be `settlementTransfersPrepare` and 
if all are successful, the state for the settlement will be updated to `PS_TRANSFERS_RECORDED`.

The following logic is applied for each `updateSettlementById` settlement event.

Retrieve settlement by settlement id:
```javascript
// seq-settlement-6.2.5, step 3
const settlementData = await knex('settlement AS s')
  .join('settlementStateChange AS ssc', 'ssc.settlementStateChangeId', 's.currentStateChangeId')
  .join('settlementModel AS sm', 'sm.settlementModelId', 's.settlementModelId')
  .select('s.settlementId', 'ssc.settlementStateId', 'ssc.reason', 'ssc.createdDate', 'sm.autoPositionReset', 'sm.requireLiquidityCheck')
  .where('s.settlementId', settlementId)
  .first()
  .transacting(trx)
  .forUpdate()
```
```sql
--1587309924793 - Retrieve affected settlement
SELECT s.settlementId, ssc.settlementStateId, ssc.reason, ssc.createdDate, sm.autoPositionReset, sm.requireLiquidityCheck
FROM settlement AS s  
LEFT JOIN settlementStateChange AS ssc ON ssc.settlementStateChangeId = s.currentStateChangeId
LEFT JOIN settlementModel AS sm ON sm.settlementModelId = s.settlementModelId  
WHERE s.settlementId = ?
```

Obtain the settlement list of accounts:
```javascript
// seq-settlement-6.2.5, step 5
const settlementAccountList = 
    await knex('settlementParticipantCurrency AS spc')
        .leftJoin('settlementParticipantCurrencyStateChange AS spcsc', 'spcsc.settlementParticipantCurrencyStateChangeId', 'spc.currentStateChangeId')
        .join('participantCurrency AS pc', 'pc.participantCurrencyId', 'spc.participantCurrencyId')
        .select('pc.participantId', 'spc.participantCurrencyId', 'spcsc.settlementStateId', 'spcsc.reason', 'spc.netAmount', 'pc.currencyId', 'spc.settlementParticipantCurrencyId AS key'
)
.where('spc.settlementId', settlementId)
.transacting(trx)
.forUpdate()

// seq-settlement-6.2.5, step 7
const settlementAccounts = {
    pendingSettlementCount: 0,
    psTransfersRecordedCount: 0,
    psTransfersReservedCount: 0,
    psTransfersCommittedCount: 0,
    settledCount: 0,
    abortedCount: 0,
    unknownCount: 0,
    settledIdList: [],
    changedIdList: []
}
```
```sql
--1587309924794 - Retrieve affected settlement accounts via settlementId
SELECT pc.participantId, spc.participantCurrencyId, spcsc.settlementStateId, spcsc.reason, spc.netAmount, pc.currencyId, spc.settlementParticipantCurrencyId AS key
FROM settlementParticipantCurrency AS spc  
LEFT JOIN settlementParticipantCurrencyStateChange AS spcsc ON spcsc.settlementParticipantCurrencyStateChangeId = spc.currentStateChangeId
JOIN participantCurrency AS pc ON pc.participantCurrencyId = spc.participantCurrencyId  
WHERE spc.settlementId = ?
```

For each of the accounts, map the state based on `participantCurrencyId`:
```javascript
// seq-settlement-6.2.5, step 8
const pid = account.participantId
const aid = account.participantCurrencyId
const state = account.settlementStateId
const allAccounts = new Map()
allAccounts[aid] = {
  id: aid,
  state,
  reason: account.reason,
  createDate: account.createdDate,
  netSettlementAmount: {
    amount: account.netAmount,
    currency: account.currencyId
  },
  participantId: pid,
  key: account.key
}
```

Count the number of accounts for each state and map to the `settlementAccounts` variable:
```javascript
// seq-settlement-6.2.5, step 9
switch (state) {
  case enums.settlementStates.PENDING_SETTLEMENT: {
    settlementAccounts.pendingSettlementCount++
    break
  }
  case enums.settlementStates.PS_TRANSFERS_RECORDED: {
    settlementAccounts.psTransfersRecordedCount++
    break
  }
  case enums.settlementStates.PS_TRANSFERS_RESERVED: {
    settlementAccounts.psTransfersReservedCount++
    break
  }
  case enums.settlementStates.PS_TRANSFERS_COMMITTED: {
    settlementAccounts.psTransfersCommittedCount++
    break
  }
  case enums.settlementStates.SETTLED: {
    settlementAccounts.settledCount++
    break
  }
  case enums.settlementStates.ABORTED: {
    settlementAccounts.abortedCount++
    break
  }
  default: {
    settlementAccounts.unknownCount++
    break
  }
}
```

Validate, map and compare each of the accounts in the payload (`participantPayload`/`REST`) with the accounts expected for settlement (`allAccounts`/`MySQL`):
```javascript
// seq-settlement-6.2.5, step 11
for (let participant in payload.participants) {
    const participantPayload = payload.participants[participant]
    participants.push({ id: participantPayload.id, accounts: [] })
    const pi = participants.length - 1
    participant = participants[pi]
    // seq-settlement-6.2.5, step 12
    for (const account in participantPayload.accounts) {
        const accountPayload = participantPayload.accounts[account]
        // seq-settlement-6.2.5, step 13
        if (allAccounts[accountPayload.id] === undefined) {
            participant.accounts.push({
                id: accountPayload.id,
                errorInformation: ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.CLIENT_ERROR, 'Account not found').toApiErrorObject().errorInformation
            })
// 'Account not found'                  - When payload account is not found in settlement account.
// 'Participant and account mismatch'   - When the payload account id does not match the expected account id.
// 'Account already processed once'     - When the current account settlement state is `SETTLED`.
// 'State change not allowed'           - When the payload state is invalid when compared to current settlement status.

// The following variables will be populated:
// # 1. settlementParticipantCurrencyStateChange: 
const spcsc = {
    settlementParticipantCurrencyId: allAccounts[accountPayload.id].key,
    settlementStateId: accountPayload.state,
    reason: accountPayload.reason,
    externalReference: accountPayload.externalReference,
    createdDate: transactionTimestamp
}
if (accountPayload.state === enums.settlementStates.PS_TRANSFERS_RECORDED) {
    spcsc.settlementTransferId = Uuid()
}
settlementParticipantCurrencyStateChange.push(spcsc);
// # 2. processedAccounts: 
//      Where the database account settlement state matches that of the payload, the following data will be mapped:
processedAccounts.push(accountPayload.id)
participant.accounts.push({
    id: accountPayload.id,
    state: accountPayload.state,
    reason: accountPayload.reason,
    externalReference: accountPayload.externalReference,
    createdDate: transactionTimestamp,
    netSettlementAmount: allAccounts[accountPayload.id].netSettlementAmount
})
settlementParticipantCurrencyStateChange.push({
    settlementParticipantCurrencyId: allAccounts[accountPayload.id].key,
    settlementStateId: accountPayload.state,
    reason: accountPayload.reason,
    externalReference: accountPayload.externalReference
})
allAccounts[accountPayload.id].reason = accountPayload.reason
allAccounts[accountPayload.id].createdDate = transactionTimestamp

// Else, the valid state transfer will be as explained in the table below:
```

| Current State in MySQL Database | Updated Valid State in payload |
|---------------------------------|--------------------------------|
| `PENDING_SETTLEMENT`            | `PS_TRANSFERS_RECORDED`        |
| `PS_TRANSFERS_RECORDED`         | `PS_TRANSFERS_RESERVED`        |
| `PS_TRANSFERS_RESERVED`         | `PS_TRANSFERS_COMMITTED`       |
| `PS_TRANSFERS_COMMITTED`        | `SETTLING`                     |
| `SETTLING`                      | `SETTLED`                      |

Counters are updated based on current and new settlement states:
```javascript
else if ((settlementData.settlementStateId === enums.settlementStates.PENDING_SETTLEMENT && accountPayload.state === enums.settlementStates.PS_TRANSFERS_RECORDED) ||
(settlementData.settlementStateId === enums.settlementStates.PS_TRANSFERS_RECORDED && accountPayload.state === enums.settlementStates.PS_TRANSFERS_RESERVED) ||
(settlementData.settlementStateId === enums.settlementStates.PS_TRANSFERS_RESERVED && accountPayload.state === enums.settlementStates.PS_TRANSFERS_COMMITTED) ||
((settlementData.settlementStateId === enums.settlementStates.PS_TRANSFERS_COMMITTED || settlementData.settlementStateId === enums.settlementStates.SETTLING) &&
  accountPayload.state === enums.settlementStates.SETTLED)) {
processedAccounts.push(accountPayload.id)
participant.accounts.push({
  id: accountPayload.id,
  state: accountPayload.state,
  reason: accountPayload.reason,
  externalReference: accountPayload.externalReference,
  createdDate: transactionTimestamp,
  netSettlementAmount: allAccounts[accountPayload.id].netSettlementAmount
})
const spcsc = {
  settlementParticipantCurrencyId: allAccounts[accountPayload.id].key,
  settlementStateId: accountPayload.state,
  reason: accountPayload.reason,
  externalReference: accountPayload.externalReference,
  createdDate: transactionTimestamp
}
if (accountPayload.state === enums.settlementStates.PS_TRANSFERS_RECORDED) {
  spcsc.settlementTransferId = Uuid()
}
settlementParticipantCurrencyStateChange.push(spcsc)

if (accountPayload.state === enums.settlementStates.PS_TRANSFERS_RECORDED) {
  settlementAccounts.pendingSettlementCount--
  settlementAccounts.psTransfersRecordedCount++
} else if (accountPayload.state === enums.settlementStates.PS_TRANSFERS_RESERVED) {
  settlementAccounts.psTransfersRecordedCount--
  settlementAccounts.psTransfersReservedCount++
} else if (accountPayload.state === enums.settlementStates.PS_TRANSFERS_COMMITTED) {
  settlementAccounts.psTransfersReservedCount--
  settlementAccounts.psTransfersCommittedCount++
} else /* if (accountPayload.state === enums.settlementStates.SETTLED) */ { // disabled as else path is never taken
  settlementAccounts.psTransfersCommittedCount--
  settlementAccounts.settledCount++
  settlementAccounts.settledIdList.push(accountPayload.id)
}
settlementAccounts.changedIdList.push(accountPayload.id)
allAccounts[accountPayload.id].state = accountPayload.state
allAccounts[accountPayload.id].reason = accountPayload.reason
allAccounts[accountPayload.id].externalReference = accountPayload.externalReference
allAccounts[accountPayload.id].createdDate = transactionTimestamp
```

The next step is to update the MySQL database with all the valid settlement state change data from the payload:
```javascript
let insertPromises = []
let updatePromises = []
// seq-settlement-6.2.5, step 19
for (const spcsc of settlementParticipantCurrencyStateChange) {
// Switched to insert from batchInsert because only LAST_INSERT_ID is returned
// TODO:: PoC - batchInsert + SELECT inserted ids vs multiple inserts without select
const spcscCopy = Object.assign({}, spcsc)
delete spcscCopy.settlementTransferId
insertPromises.push(
  knex('settlementParticipantCurrencyStateChange')
    .insert(spcscCopy)
    .transacting(trx)
)
}
const settlementParticipantCurrencyStateChangeIdList = (await Promise.all(insertPromises)).map(v => v[0])
// seq-settlement-6.2.5, step 21
for (const i in settlementParticipantCurrencyStateChangeIdList) {
const updatedColumns = { currentStateChangeId: settlementParticipantCurrencyStateChangeIdList[i] }
if (settlementParticipantCurrencyStateChange[i].settlementTransferId) {
  updatedColumns.settlementTransferId = settlementParticipantCurrencyStateChange[i].settlementTransferId
}
updatePromises.push(
  knex('settlementParticipantCurrency')
    .where('settlementParticipantCurrencyId', settlementParticipantCurrencyStateChange[i].settlementParticipantCurrencyId)
    .update(updatedColumns)
    .transacting(trx)
)
}
await Promise.all(updatePromises)
```
```sql
--1587309924795 - Insert new `settlementParticipantCurrencyStateChange`:
INSERT INTO settlementParticipantCurrencyStateChange
    (settlementParticipantCurrencyId, settlementStateId, reason, externalReference) VALUES 
    (?,--allAccounts[accountPayload.id].key
    ?,--accountPayload.state (The new state)
    ?,--accountPayload.reason (The new reason)
    ?)--accountPayload.externalReference (Updated external ref)
--1587309924796 - Update the `settlementParticipantCurrency` to point to the latest status:
UPDATE settlementParticipantCurrency SET 
 currentStateChangeId = ?,--Id's as inserted into `settlementParticipantCurrencyStateChange`
 settlementTransferId = ?--Newly created transferId, but only if payload was state [PS_TRANSFERS_RECORDED]
WHERE settlementParticipantCurrencyId = ?--Accounts created as part of settlement event trigger.
```

Only in the event of `autoPositionReset` flag being set to `true`, the Settlement events will progress the settlement status 
based on the existing settlement status. 
> Please see below sections for each of the actions that will be taken for [PENDING_SETTLEMENT, PS_TRANSFERS_RECORDED, PS_TRANSFERS_RESERVED]:

The logic below describes how each of the settlement events are triggered when `autoPositionReset` is active:
```javascript
if (settlementData.settlementStateId === enums.settlementStates.PENDING_SETTLEMENT) {
  await Facade.settlementTransfersPrepare(settlementId, transactionTimestamp, enums, trx)
} else if (settlementData.settlementStateId === enums.settlementStates.PS_TRANSFERS_RECORDED) {
  await Facade.settlementTransfersReserve(settlementId, transactionTimestamp, requireLiquidityCheck, enums, trx)
} else if (settlementData.settlementStateId === enums.settlementStates.PS_TRANSFERS_RESERVED) {
  await Facade.settlementTransfersCommit(settlementId, transactionTimestamp, enums, trx)
}
```

#### Settlement Event - Record Transfers `(PENDING_SETTLEMENT -> PS_TRANSFERS_RECORDED)`
Process the settlement for payee.
The initial `autoPositionReset` settlement event is triggered via `updateSettlementById`, 
the settlement will be in a state of `PENDING_SETTLEMENT` as created by `settlementEventTrigger`.
Once the `updateSettlementById` endpoint is invoked, the `settlementTransfersPrepare` function will be consumed 
(due to existing `PENDING_SETTLEMENT` state).

Retrieve the list of accounts with no settlement transactions:
```javascript
// Retrieve list of PS_TRANSFERS_RECORDED, but not RECEIVED_PREPARE
const settlementTransferList = await knex('settlementParticipantCurrency AS spc')
.join('settlementParticipantCurrencyStateChange AS spcsc', 'spcsc.settlementParticipantCurrencyId', 'spc.settlementParticipantCurrencyId')
.join('participantCurrency AS pc', 'pc.participantCurrencyId', 'spc.participantCurrencyId')
.leftJoin('transferDuplicateCheck AS tdc', 'tdc.transferId', 'spc.settlementTransferId')
.select('spc.*', 'pc.currencyId')
.where('spc.settlementId', settlementId)
.where('spcsc.settlementStateId', enums.settlementStates.PS_TRANSFERS_RECORDED)
.whereNotNull('spc.settlementTransferId')
.whereNull('tdc.transferId')
.transacting(trx)
```
```sql
--1587309924797 - Retrieve all transactions with state `PS_TRANSFERS_RECORDED`, created as part of current transaction context:
SELECT spc.*, pc.currencyId 
FROM settlementParticipantCurrency AS spc
JOIN settlementParticipantCurrencyStateChange AS spcsc ON spcsc.settlementParticipantCurrencyId = spc.settlementParticipantCurrencyId
JOIN participantCurrency AS pc ON pc.participantCurrencyId = spc.participantCurrencyId
LEFT JOIN transferDuplicateCheck AS tdc ON tdc.transferId = spc.settlementTransferId
WHERE 
    spc.settlementId = ?--Current settlement
AND
    spcsc.settlementStateId = enums.settlementStates.PS_TRANSFERS_RECORDED
AND
    spc.settlementTransferId IS NOT NULL--Populated due to [PS_TRANSFERS_RECORDED] event
AND
    tdc.transferId IS NULL
```

Insert `transferDuplicateCheck` and `transfer` for settlement:
```javascript
const hashSha256 = Crypto.createHash('sha256')
let hash = hashSha256.update(String(t.settlementTransferId))
hash = hashSha256.digest(hash).toString('base64').slice(0, -1) // removing the trailing '=' as per the specification
// Insert transferDuplicateCheck
await knex('transferDuplicateCheck')
.insert({
  transferId: t.settlementTransferId,
  hash,
  createdDate: transactionTimestamp
})
.transacting(trx)
// Insert transfer
await knex('transfer')
    .insert({
        transferId: t.settlementTransferId,
        amount: Math.abs(t.netAmount),
        currencyId: t.currencyId,
        ilpCondition: 0,
        expirationDate: new Date(+new Date() + 1000 * Number(Config.TRANSFER_VALIDITY_SECONDS)).toISOString().replace(/[TZ]/g, ' ').trim(),
        createdDate: transactionTimestamp
    })
    .transacting(trx)
```
```sql
--1687309924799 - Insert `transferDuplicateCheck`:
INSERT INTO transferDuplicateCheck
    (transferId, hash, createdDate) VALUES 
    (?,--t.settlementTransferId
    ?, --hash variable
    ?) --transactionTimestamp variable
--1787309924700 - Update the `settlementParticipantCurrency` to point to the latest status:
INSERT INTO transfer
    (transferId, amount, currencyId, ilpCondition, expirationDate, createdDate) VALUES
    (?,--t.settlementTransferId
    ?, --Math.abs(t.netAmount) - current value from `settlementParticipantCurrency.netAmount` 
    ?, --current value from `settlementParticipantCurrency.currencyId`
    0, --hardcoded to `0`
    ?, --new Date(+new Date() + 1000 * Number(Config.TRANSFER_VALIDITY_SECONDS)).toISOString().replace(/[TZ]/g, ' ').trim()
    ?) --transactionTimestamp variable
``` 

Create `transferParticipant` records to debit the position (`DFSP POSITION`) and credit the hub (`HUB`) accounts and also create the latest `transferStateChange` record:
```javascript
// Retrieve Hub mlns account
const { mlnsAccountId } = await knex('participantCurrency AS pc1')
.join('participantCurrency AS pc2', function () {
  this.on('pc2.participantId', Config.HUB_ID)
    .andOn('pc2.currencyId', 'pc1.currencyId')
    .andOn('pc2.ledgerAccountTypeId', enums.ledgerAccountTypes.HUB_MULTILATERAL_SETTLEMENT)
    .andOn('pc2.isActive', 1)
})
.select('pc2.participantCurrencyId AS mlnsAccountId')
.where('pc1.participantCurrencyId', t.participantCurrencyId)
.first()
.transacting(trx)

let ledgerEntryTypeId
if (t.netAmount < 0) {
ledgerEntryTypeId = enums.ledgerEntryTypes.SETTLEMENT_NET_RECIPIENT
} else if (t.netAmount > 0) {
ledgerEntryTypeId = enums.ledgerEntryTypes.SETTLEMENT_NET_SENDER
} else { // t.netAmount === 0
ledgerEntryTypeId = enums.ledgerEntryTypes.SETTLEMENT_NET_ZERO
}

// Insert transferParticipant records
await knex('transferParticipant')
.insert({
    transferId: t.settlementTransferId,
    participantCurrencyId: mlnsAccountId,
    transferParticipantRoleTypeId: enums.transferParticipantRoleTypes.HUB,
    ledgerEntryTypeId,
    amount: t.netAmount,
    createdDate: transactionTimestamp
})
.transacting(trx)
await knex('transferParticipant')
.insert({
    transferId: t.settlementTransferId,
    participantCurrencyId: t.participantCurrencyId,
    transferParticipantRoleTypeId: enums.transferParticipantRoleTypes.DFSP_POSITION,
    ledgerEntryTypeId,
    amount: -t.netAmount,
    createdDate: transactionTimestamp
})
.transacting(trx)

// Insert transferStateChange
await knex('transferStateChange')
.insert({
    transferId: t.settlementTransferId,
    transferStateId: enums.transferStates.RECEIVED_PREPARE,
    reason: 'Settlement transfer prepare',
    createdDate: transactionTimestamp
})
.transacting(trx)
```
```sql
--1487309924700 - Retrieve the [participantCurrencyId] for the Hub Multilateral Settlement account: 
SELECT participantCurrencyId--Mapping for `mlnsAccountId`
FROM participantCurrency AS pc1
 JOIN participantCurrency AS pc2 
     ON pc2.participantId = Config.HUB_ID 
            AND pc2.currencyId = pc1.currencyId
            AND pc2.ledgerAccountTypeId = enums.ledgerAccountTypes.HUB_MULTILATERAL_SETTLEMENT
            AND pc2.isActive = 1
WHERE pc1.participantCurrencyId = ?--t.participantCurrencyId
--1487309924701 - Insert [credit] for enums.transferParticipantRoleTypes.HUB
INSERT INTO transferParticipant (transferId, participantCurrencyId, transferParticipantRoleTypeId, ledgerEntryTypeId, amount, createdDate)
VALUES
    (?,--t.settlementTransferId
    ?, --mlnsAccountId -> Obtained from lookup (HUB)
    enums.transferParticipantRoleTypes.HUB, 
    ?, --ledgerEntryTypeId (Based on the [t.netAmount])
    ?, --t.netAmount
    ?) --transactionTimestamp
--1487309924702 - Another Insert for [debit] on enums.transferParticipantRoleTypes.DFSP_POSITION
INSERT INTO transferParticipant (transferId, participantCurrencyId, transferParticipantRoleTypeId, ledgerEntryTypeId, amount, createdDate)
VALUES
    (?,--t.settlementTransferId
    ?, --t.participantCurrencyId -> Each settlement account created for settlement window.
    enums.transferParticipantRoleTypes.DFSP_POSITION,
    ?, --ledgerEntryTypeId (Based on the [t.netAmount])
    ?, -- -t.netAmount (The debit on the DFSP)
    ?) --transactionTimestamp
--1487309924703 - Record the state change in `transferStateChange`:
INSERT INTO transferStateChange (transferId, transferStateId, reason, createdDate)
VALUES
    (?,--t.settlementTransferId
    enums.transferStates.RECEIVED_PREPARE, --enums.transferStates.RECEIVED_PREPARE
    'Settlement transfer prepare', --hardcoded reason
    ?) --transactionTimestamp
```

#### Settlement Event - Reservation `(PS_TRANSFERS_RECORDED -> PS_TRANSFERS_RESERVED)`
Process the settlement reservation for payer.
The second `autoPositionReset` settlement event is triggered via `updateSettlementById`,
the settlement will be in a state of `PS_TRANSFERS_RECORDED` as created by the initial `updateSettlementById`.
Once the `updateSettlementById` endpoint is invoked, the `settlementTransfersReserve` function will be consumed
(due to existing `PS_TRANSFERS_RECORDED` state).

Retrieve list of `PS_TRANSFERS_RESERVED`, but not `RESERVED`:
```javascript
const settlementTransferList = await knex('settlementParticipantCurrency AS spc')
.join('settlementParticipantCurrencyStateChange AS spcsc', function () {
  this.on('spcsc.settlementParticipantCurrencyId', 'spc.settlementParticipantCurrencyId')
    .andOn('spcsc.settlementStateId', knex.raw('?', [enums.settlementStates.PS_TRANSFERS_RESERVED]))
})
.join('transferStateChange AS tsc1', function () {
  this.on('tsc1.transferId', 'spc.settlementTransferId')
    .andOn('tsc1.transferStateId', knex.raw('?', [enums.transferStates.RECEIVED_PREPARE]))
})
.leftJoin('transferStateChange AS tsc2', function () {
  this.on('tsc2.transferId', 'spc.settlementTransferId')
    .andOn('tsc2.transferStateId', knex.raw('?', [enums.transferStates.RESERVED]))
})
.join('transferParticipant AS tp1', function () {
  this.on('tp1.transferId', 'spc.settlementTransferId')
    .andOn('tp1.transferParticipantRoleTypeId', knex.raw('?', [enums.transferParticipantRoleTypes.DFSP_POSITION]))
})
.join('participantCurrency AS pc1', 'pc1.participantCurrencyId', 'tp1.participantCurrencyId')
.join('participant AS p1', 'p1.participantId', 'pc1.participantId')
.join('transferParticipant AS tp2', function () {
  this.on('tp2.transferId', 'spc.settlementTransferId')
    .andOn('tp2.transferParticipantRoleTypeId', knex.raw('?', [enums.transferParticipantRoleTypes.HUB]))
})
.select('tp1.transferId', 'tp1.ledgerEntryTypeId', 'tp1.participantCurrencyId AS dfspAccountId', 'tp1.amount AS dfspAmount',
  'tp2.participantCurrencyId AS hubAccountId', 'tp2.amount AS hubAmount',
  'p1.name AS dfspName', 'pc1.currencyId')
.where('spc.settlementId', settlementId)
.whereNull('tsc2.transferId')
.transacting(trx)
```
```sql
--1487309924800 - Retrieve `PS_TRANSFERS_RESERVED` 
SELECT tp1.transferId, tp1.ledgerEntryTypeId, tp1.participantCurrencyId AS dfspAccountId, tp1.amount AS dfspAmount 
FROM settlementParticipantCurrency AS spc
JOIN settlementParticipantCurrencyStateChange AS spcsc ON 
    spcsc.settlementParticipantCurrencyId = spc.settlementParticipantCurrencyId AND
    spcsc.settlementStateId = enums.settlementStates.PS_TRANSFERS_RESERVED
JOIN transferStateChange AS tsc1 ON
    tsc1.transferId = spc.settlementTransferId AND
    tsc1.transferStateId = enums.transferStates.RECEIVED_PREPARE
LEFT JOIN transferStateChange AS tsc2 ON
    tsc2.transferId = spc.settlementTransferId AND
    tsc1.transferStateId = enums.transferStates.RESERVED
LEFT JOIN transferParticipant AS tp1 ON
    tp1.transferId = spc.settlementTransferId AND
    tp1.transferParticipantRoleTypeId = enums.transferParticipantRoleTypes.DFSP_POSITION
JOIN participantCurrency AS pc1 ON pc1.participantCurrencyId = tp1.participantCurrencyId
JOIN participant AS p1 ON p1.participantId = pc1.participantId
JOIN transferParticipant AS tp2 ON 
    tp2.transferId = spc.settlementTransferId AND
    tp2.transferParticipantRoleTypeId = enums.transferParticipantRoleTypes.HUB
LEFT JOIN transferDuplicateCheck AS tdc ON tdc.transferId = spc.settlementTransferId
WHERE 
    spc.settlementId = ?--Current settlement
AND
    tsc2.transferId IS NULL
```

Record the state change for each of the transfers obtained from the previous step (record transfers):
```javascript
// Persist transfer state change
transferStateChangeId = await knex('transferStateChange')
  .insert({
    transferId,
    transferStateId: enums.transferStates.RESERVED,
    reason: 'Settlement transfer reserve',
    createdDate: transactionTimestamp
  })
  .transacting(trx)

// The following logic will only apply if (ledgerEntryTypeId === enums.ledgerEntryTypes.SETTLEMENT_NET_RECIPIENT):
// Which basically implies that the DFSP `netAmount is less than 0` 
// Select dfspPosition FOR UPDATE
const {dfspPositionId, dfspPositionValue, dfspReservedValue} = await knex('participantPosition')
    .select('participantPositionId AS dfspPositionId', 'value AS dfspPositionValue', 'reservedValue AS dfspReservedValue')
    .where('participantCurrencyId', dfspAccountId)

if (requireLiquidityCheck) {
    // Select dfsp NET_DEBIT_CAP limit
    const {netDebitCap} = await knex('participantLimit')
        .select('value AS netDebitCap')
        .where('participantCurrencyId', dfspAccountId)
        .andWhere('participantLimitTypeId', enums.participantLimitTypes.NET_DEBIT_CAP)
    isLimitExceeded = netDebitCap - dfspPositionValue - dfspReservedValue - dfspAmount < 0
    if (isLimitExceeded) {
        await knex('participantPositionChange')
            .select('participantPositionChangeId AS startAfterParticipantPositionChangeId')
            .where('participantPositionId', dfspPositionId)
            .orderBy('participantPositionChangeId', 'desc')
        await ParticipantFacade.adjustLimits(dfspAccountId, {
            type: 'NET_DEBIT_CAP',
            value: new MLNumber(netDebitCap).add(dfspAmount).toNumber()
        }, trx)
    }
}

// Persist dfsp latestPosition
await knex('participantPosition')
    .update('value', new MLNumber(dfspPositionValue).add(dfspAmount).toNumber())
    .where('participantPositionId', dfspPositionId)
    .transacting(trx)
// Persist dfsp position change
await knex('participantPositionChange')
    .insert({
        participantPositionId: dfspPositionId,
        transferStateChangeId,
        value: new MLNumber(dfspPositionValue).add(dfspAmount).toNumber(),
        reservedValue: dfspReservedValue,
        createdDate: transactionTimestamp
    })
    .transacting(trx)

// Send notification for position change
const action = 'settlement-transfer-position-change'
const destination = dfspName
const payload = {
    currency: currencyId,
    value: new MLNumber(dfspPositionValue).add(dfspAmount).toNumber(),
    changedDate: new Date().toISOString()
}
const message = Facade.getNotificationMessage(action, destination, payload)
await Utility.produceGeneralMessage(Utility.ENUMS.NOTIFICATION, Utility.ENUMS.EVENT, message, Utility.ENUMS.STATE.SUCCESS)

// Select hubPosition FOR UPDATE
const {hubPositionId, hubPositionValue} = await knex('participantPosition')
    .select('participantPositionId AS hubPositionId', 'value AS hubPositionValue')
    .where('participantCurrencyId', hubAccountId)

// Persist hub latestPosition
await knex('participantPosition')
    .update('value', new MLNumber(hubPositionValue).add(hubAmount).toNumber())
    .where('participantPositionId', hubPositionId)

// Persist hub position change
await knex('participantPositionChange')
    .insert({
        participantPositionId: hubPositionId,
        transferStateChangeId,
        value: new MLNumber(hubPositionValue).add(hubAmount).toNumber(),
        reservedValue: 0,
        createdDate: transactionTimestamp
    })
```
```sql
--1587309924700 - Insert the state change to `RESERVED`
INSERT INTO transferStateChange (transferId, transferStateId, reason, createdDate)
VALUES
    (?,--transferId from the previous query lookup
     enums.transferStates.RESERVED,
    'Settlement transfer reserve', --hardcoded reason
    ?) --transactionTimestamp
--1587309924701 - NB: The remainder of SQL queries below is only for when `ledgerEntryTypeId === enums.ledgerEntryTypes.SETTLEMENT_NET_RECIPIENT`
--Lookup the `participantPosition` in order to update:
SELECT participantPositionId AS dfspPositionId, value AS dfspPositionValue, reservedValue AS dfspReservedValue
FROM participantPosition
WHERE participantCurrencyId = ?;--dfspAccountId - The `enums.transferParticipantRoleTypes.DFSP_POSITION` account for [Payee]
--1587309924702 - NB: The below is only for `requireLiquidityCheck`:
SELECT value AS netDebitCap
FROM participantLimit
WHERE participantCurrencyId = ?--dfspAccountId - The `enums.transferParticipantRoleTypes.DFSP_POSITION` account for [Payee]
AND
    participantLimitTypeId = enums.participantLimitTypes.NET_DEBIT_CAP;
--The above ^^^ is for limit exceed check: isLimitExceeded = netDebitCap - dfspPositionValue - dfspReservedValue - dfspAmount < 0
--NB: The below is only for when the limit is exceeded i.e `isLimitExceeded === true`
--1587309924703 - NB: The below is only for `isLimitExceeded`:
--The below query is ran, but serves no purpose, as it was suppose to set [startAfterParticipantPositionChangeId] (to be removed):
SELECT participantPositionChangeId AS startAfterParticipantPositionChangeId
FROM participantPositionChange
WHERE participantPositionId = ?--dfspPositionId - The `enums.transferParticipantRoleTypes.DFSP_POSITION` account for [Payee]
ORDER BY participantPositionChangeId DESC;
--1587309924704 - Update the DFSP latest Position:
UPDATE participantPosition SET
    value = ?--new MLNumber(dfspPositionValue).add(dfspAmount).toNumber())
WHERE participantPositionId = ?--dfspPositionId
--1587309924705 - Persist DFSP position change:
INSERT INTO participantPositionChange (participantPositionId, transferStateChangeId, value, reservedValue, createdDate) VALUES (
    ?,--dfspPositionId
    ?,--transferStateChangeId - The Id from the [transferStateChange] at the start of the SQL.
    ?,--new MLNumber(dfspPositionValue).add(dfspAmount).toNumber()
    ?,--dfspReservedValue - Unchanged from lookup
    ?);--transactionTimestamp
--At this point, the notification message is sent to the DFSP.
--1587309924706 - Select hubPosition FOR UPDATE:
SELECT participantPositionId AS hubPositionId, value AS hubPositionValue FROM participantPosition
WHERE participantCurrencyId = ?--hubAccountId - The Hub account per currency and settlement.
--1587309924707 - Persist hub position change:
INSERT INTO participantPositionChange (participantPositionId, transferStateChangeId, value, reservedValue, createdDate) VALUES (
    ?,--hubPositionId
    ?,--transferStateChangeId - The Id from the [transferStateChange] at the start of the SQL.
    ?,--new MLNumber(hubPositionValue).add(hubAmount).toNumber()
    0,--Reserved value hardcoded to `0`.
    ?);--transactionTimestamp
```

#### Settlement Event - Commit `(PS_TRANSFERS_RESERVED -> PS_TRANSFERS_COMMITTED)`
Process the settlement commit for payer.
The third and final `autoPositionReset` settlement event is triggered via `updateSettlementById`,
the settlement will be in a state of `PS_TRANSFERS_RESERVED` as created by the second `updateSettlementById` invocation.
Once the `updateSettlementById` endpoint is invoked, the `settlementTransfersCommit` function will be consumed
(due to existing `PS_TRANSFERS_RESERVED` state).

Retrieve list of `PS_TRANSFERS_COMMITTED`, but not `COMMITTED`:
```javascript
  const settlementTransferList = await knex('settlementParticipantCurrency AS spc')
    .join('settlementParticipantCurrencyStateChange AS spcsc', function () {
      this.on('spcsc.settlementParticipantCurrencyId', 'spc.settlementParticipantCurrencyId')
        .andOn('spcsc.settlementStateId', knex.raw('?', [enums.settlementStates.PS_TRANSFERS_COMMITTED]))
    })
    .join('transferStateChange AS tsc1', function () {
      this.on('tsc1.transferId', 'spc.settlementTransferId')
        .andOn('tsc1.transferStateId', knex.raw('?', [enums.transferStates.RESERVED]))
    })
    .leftJoin('transferStateChange AS tsc2', function () {
      this.on('tsc2.transferId', 'spc.settlementTransferId')
        .andOn('tsc2.transferStateId', knex.raw('?', [enums.transferStates.COMMITTED]))
    })
    .join('transferParticipant AS tp1', function () {
      this.on('tp1.transferId', 'spc.settlementTransferId')
        .andOn('tp1.transferParticipantRoleTypeId', knex.raw('?', [enums.transferParticipantRoleTypes.DFSP_POSITION]))
    })
    .join('participantCurrency AS pc1', 'pc1.participantCurrencyId', 'tp1.participantCurrencyId')
    .join('participant AS p1', 'p1.participantId', 'pc1.participantId')
    .join('transferParticipant AS tp2', function () {
      this.on('tp2.transferId', 'spc.settlementTransferId')
        .andOn('tp2.transferParticipantRoleTypeId', knex.raw('?', [enums.transferParticipantRoleTypes.HUB]))
    })
    .select('tp1.transferId', 'tp1.ledgerEntryTypeId', 'tp1.participantCurrencyId AS dfspAccountId', 'tp1.amount AS dfspAmount',
      'tp2.participantCurrencyId AS hubAccountId', 'tp2.amount AS hubAmount',
      'p1.name AS dfspName', 'pc1.currencyId')
    .where('spc.settlementId', settlementId)
    .whereNull('tsc2.transferId')
    .transacting(trx)
```
```sql
--1497309924800 - Retrieve `PS_TRANSFERS_COMMITTED` 
SELECT tp1.transferId, tp1.ledgerEntryTypeId, tp1.participantCurrencyId AS dfspAccountId, tp1.amount AS dfspAmount 
FROM settlementParticipantCurrency AS spc
JOIN settlementParticipantCurrencyStateChange AS spcsc ON 
    spcsc.settlementParticipantCurrencyId = spc.settlementParticipantCurrencyId AND
    spcsc.settlementStateId = enums.settlementStates.PS_TRANSFERS_COMMITTED
JOIN transferStateChange AS tsc1 ON
    tsc1.transferId = spc.settlementTransferId AND
    tsc1.transferStateId = enums.transferStates.RESERVED
LEFT JOIN transferStateChange AS tsc2 ON
    tsc2.transferId = spc.settlementTransferId AND
    tsc1.transferStateId = enums.transferStates.COMMITTED
LEFT JOIN transferParticipant AS tp1 ON
    tp1.transferId = spc.settlementTransferId AND
    tp1.transferParticipantRoleTypeId = enums.transferParticipantRoleTypes.DFSP_POSITION
JOIN participantCurrency AS pc1 ON pc1.participantCurrencyId = tp1.participantCurrencyId
JOIN participant AS p1 ON p1.participantId = pc1.participantId
JOIN transferParticipant AS tp2 ON 
    tp2.transferId = spc.settlementTransferId AND
    tp2.transferParticipantRoleTypeId = enums.transferParticipantRoleTypes.HUB
LEFT JOIN transferDuplicateCheck AS tdc ON tdc.transferId = spc.settlementTransferId
WHERE 
    spc.settlementId = ?--Current settlement
AND
    tsc2.transferId IS NULL
```

Record the state change for each of the transfers obtained from the previous step (reservation):
```javascript
// Persist transfer fulfilment and transfer state change
await knex('transferFulfilmentDuplicateCheck').insert({transferId})

await knex('transferFulfilment')
  .insert({
    transferId,
    ilpFulfilment: 0,
    completedDate: transactionTimestamp,
    isValid: 1,
    settlementWindowId: null,
    createdDate: transactionTimestamp
  })

await knex('transferStateChange')
  .insert({
    transferId,
    transferStateId: enums.transferStates.RECEIVED_FULFIL,
    reason: 'Settlement transfer commit initiated',
    createdDate: transactionTimestamp
  })

transferStateChangeId = await knex('transferStateChange')
  .insert({
    transferId,
    transferStateId: enums.transferStates.COMMITTED,
    reason: 'Settlement transfer commit',
    createdDate: transactionTimestamp
  })

// The following logic will only apply if (ledgerEntryTypeId === enums.ledgerEntryTypes.SETTLEMENT_NET_SENDER):
// Which basically implies that the DFSP `netAmount is greater than 0`

// Select dfspPosition FOR UPDATE
const { dfspPositionId, dfspPositionValue, dfspReservedValue } = await knex('participantPosition')
.select('participantPositionId AS dfspPositionId', 'value AS dfspPositionValue', 'reservedValue AS dfspReservedValue')
.where('participantCurrencyId', dfspAccountId)

// Persist dfsp latestPosition
await knex('participantPosition')
.update('value', new MLNumber(dfspPositionValue).add(dfspAmount).toNumber())
.where('participantPositionId', dfspPositionId)

// Persist dfsp position change
await knex('participantPositionChange')
.insert({
  participantPositionId: dfspPositionId,
  transferStateChangeId,
  value: new MLNumber(dfspPositionValue).add(dfspAmount).toNumber(),
  reservedValue: dfspReservedValue,
  createdDate: transactionTimestamp
})

// Select hubPosition FOR UPDATE
const { hubPositionId, hubPositionValue } = await knex('participantPosition')
.select('participantPositionId AS hubPositionId', 'value AS hubPositionValue')
.where('participantCurrencyId', hubAccountId)

// Persist hub latestPosition
await knex('participantPosition')
.update('value', new MLNumber(hubPositionValue).add(hubAmount).toNumber())
.where('participantPositionId', hubPositionId)

// Persist hub position change
await knex('participantPositionChange')
.insert({
  participantPositionId: hubPositionId,
  transferStateChangeId,
  value: new MLNumber(hubPositionValue).add(hubAmount).toNumber(),
  reservedValue: 0,
  createdDate: transactionTimestamp
})

// Send notification for position change
const action = 'settlement-transfer-position-change'
const destination = dfspName
const payload = {
  currency: currencyId,
  value: new MLNumber(dfspPositionValue).add(dfspAmount).toNumber(),
  changedDate: new Date().toISOString()
}
const message = Facade.getNotificationMessage(action, destination, payload)
await Utility.produceGeneralMessage(Utility.ENUMS.NOTIFICATION, Utility.ENUMS.EVENT, message, Utility.ENUMS.STATE.SUCCESS)
```
```sql
--1497309928800 - Insert the fulfilment duplicate check: 
INSERT INTO transferFulfilmentDuplicateCheck (transferId) VALUES (?)--transferId from parameters.
--1497309928801 - Insert the transferFulfilment:
INSERT INTO transferFulfilment (transferId, ilpFulfilment, completedDate, isValid, settlementWindowId, createdDate) VALUES (
  ?,--transferId from parameters.
  0,--ilpFulfilment
  ?,--completedDate -> transactionTimestamp
  1,--isValid
  null,--settlementWindowId
  ?)--completedDate -> transactionTimestamp
--1497309928802 - Insert the transferStateChange for [enums.transferStates.RECEIVED_FULFIL]:
INSERT INTO transferStateChange (transferId, transferStateId, reason, createdDate)
VALUES
    (?,--transferId from the previous query lookup
    enums.transferStates.RECEIVED_FULFIL,
    'Settlement transfer commit initiated', --hardcoded reason
    ?) --transactionTimestamp
--1497309928803 - Insert the transferStateChange for [enums.transferStates.COMMITTED]:
INSERT INTO transferStateChange (transferId, transferStateId, reason, createdDate)
VALUES
    (?,--transferId from the previous query lookup
    enums.transferStates.COMMITTED,
    'Settlement transfer commit initiated', --hardcoded reason
    ?) --transactionTimestamp
--1497309928804 - NB: The remainder of SQL queries below is only for when `ledgerEntryTypeId === enums.ledgerEntryTypes.SETTLEMENT_NET_SENDER`
--Lookup the `participantPosition` in order to update:
SELECT participantPositionId AS dfspPositionId, value AS dfspPositionValue, reservedValue AS dfspReservedValue
FROM participantPosition
WHERE participantCurrencyId = ?;--dfspAccountId - The `enums.transferParticipantRoleTypes.DFSP_POSITION` account for [Payer]
--1497309928805 - Persist DFSP latest position:
UPDATE participantPosition SET
    value = ?--new MLNumber(dfspPositionValue).add(dfspAmount).toNumber()
WHERE participantPositionId = ?--dfspPositionId
--1587309924706 - Persist DFSP position change:
    INSERT INTO participantPositionChange (participantPositionId, transferStateChangeId, value, reservedValue, createdDate) VALUES (
    ?,--dfspPositionId
    ?,--transferStateChangeId - The Id from the [transferStateChange] at the start of the SQL.
    ?,--new MLNumber(dfspPositionValue).add(dfspAmount).toNumber()
    ?,--dfspReservedValue - Unchanged from lookup
    ?);--transactionTimestamp
--1587309924707 - Select hubPosition FOR UPDATE:
SELECT participantPositionId AS hubPositionId, value AS hubPositionValue FROM participantPosition
WHERE participantCurrencyId = ?--hubAccountId - The Hub account per currency and settlement.
--1587309924708 - Persist hub position change:
    INSERT INTO participantPositionChange (participantPositionId, transferStateChangeId, value, reservedValue, createdDate) VALUES (
    ?,--hubPositionId
    ?,--transferStateChangeId - The Id from the [transferStateChange] at the start of the SQL.
    ?,--new MLNumber(hubPositionValue).add(hubAmount).toNumber()
    0,--Reserved value hardcoded to `0`.
    ?);--transactionTimestamp
--At this point, the notification message is sent to the DFSP.
```

### Settlement Continued
The following logic will take place post each of the following Settlement Events described above (Record Transfers, Reservation and Commit):

Perform settlement aggregation for each of the settled account ids (`settledIdList`):
(The `settlementAccounts.settledIdList` originates from the `accountPayload.state === enums.settlementStates.SETTLED`.)
```javascript
// seq-settlement-6.2.5, step 23
await knex('settlementContentAggregation').transacting(trx)
  .where('settlementId', settlementId)
  .whereIn('participantCurrencyId', settlementAccounts.settledIdList)
  .update('currentStateId', enums.settlementWindowStates.SETTLED)

// check for settled content
const scaContentToCheck = await knex('settlementContentAggregation').transacting(trx)
  .where('settlementId', settlementId)
  .whereIn('participantCurrencyId', settlementAccounts.settledIdList)
  .distinct('settlementWindowContentId')
const contentIdCheckList = scaContentToCheck.map(v => v.settlementWindowContentId)
const unsettledContent = await knex('settlementContentAggregation').transacting(trx)
  .whereIn('settlementWindowContentId', contentIdCheckList)
  .whereNot('currentStateId', enums.settlementWindowStates.SETTLED)
  .distinct('settlementWindowContentId')
const unsettledContentIdList = unsettledContent.map(v => v.settlementWindowContentId)
const settledContentIdList = arrayDiff(contentIdCheckList, unsettledContentIdList)

// persist settled content
insertPromises = []
for (const settlementWindowContentId of settledContentIdList) {
  const swcsc = {
    settlementWindowContentId,
    settlementWindowStateId: enums.settlementWindowStates.SETTLED,
    reason: 'All content aggregation records are SETTLED'
  }
  insertPromises.push(
    knex('settlementWindowContentStateChange').transacting(trx)
      .insert(swcsc)
  )
}
const settlementWindowContentStateChangeIdList = (await Promise.all(insertPromises)).map(v => v[0])
updatePromises = []
for (const i in settlementWindowContentStateChangeIdList) {
  const updatedColumns = { currentStateChangeId: settlementWindowContentStateChangeIdList[i] }
  updatePromises.push(
    knex('settlementWindowContent').transacting(trx)
      .where('settlementWindowContentId', settledContentIdList[i])
      .update(updatedColumns)
  )
}
await Promise.all(updatePromises)

// check for settled windows
const windowsToCheck = await knex('settlementWindowContent').transacting(trx)
  .whereIn('settlementWindowContentId', settledContentIdList)
  .distinct('settlementWindowId')
const windowIdCheckList = windowsToCheck.map(v => v.settlementWindowId)
const unsettledWindows = await knex('settlementWindowContent AS swc').transacting(trx)
  .join('settlementWindowContentStateChange AS swcsc', 'swcsc.settlementWindowContentStateChangeId', 'swc.currentStateChangeId')
  .whereIn('swc.settlementWindowId', windowIdCheckList)
  .whereNot('swcsc.settlementWindowStateId', enums.settlementWindowStates.SETTLED)
  .distinct('swc.settlementWindowId')
const unsettledWindowIdList = unsettledWindows.map(v => v.settlementWindowId)
const settledWindowIdList = arrayDiff(windowIdCheckList, unsettledWindowIdList)

// persist settled windows
insertPromises = []
for (const settlementWindowId of settledWindowIdList) {
  const swsc = {
    settlementWindowId,
    settlementWindowStateId: enums.settlementWindowStates.SETTLED,
    reason: 'All settlement window content is SETTLED'
  }
  insertPromises.push(
    knex('settlementWindowStateChange').transacting(trx)
      .insert(swsc)
  )
}
const settlementWindowStateChangeIdList = (await Promise.all(insertPromises)).map(v => v[0])
updatePromises = []
for (const i in settlementWindowStateChangeIdList) {
  const updatedColumns = { currentStateChangeId: settlementWindowStateChangeIdList[i] }
  updatePromises.push(
    knex('settlementWindow').transacting(trx)
      .where('settlementWindowId', settledWindowIdList[i])
      .update(updatedColumns)
  )
}
await Promise.all(updatePromises)
```
```sql
--1587319924700 - Set the settlement event for the `settlementContentAggregation`:
UPDATE settlementContentAggregation SET currentStateId = enums.settlementWindowStates.SETTLED
WHERE settlementId = ?--from the settlement.
AND participantCurrencyId IN (?);--settlementAccounts.settledIdList
--1587319924701 - Obtain all `settlementWindowContentId` for the settled accounts:
SELECT settlementWindowContentId FROM settlementContentAggregation WHERE settlementId = ?;--from the settlement.
--1587319924702 - Obtain all unsettled content:
SELECT DISTINCT settlementWindowContentId FROM settlementContentAggregation WHERE
    settlementWindowContentId IN ? --Distinct id's from [scaContentToCheck]                                                                       
AND
    currentStateId != enums.settlementWindowStates.SETTLED;
--1587319924703 - Insert the settlementWindowContentStateChange for all the new settlements:
INSERT INTO settlementWindowContentStateChange (settlementWindowContentId, settlementWindowStateId, reason) VALUES(
    ?,--settlementWindowContentId from the [settlementWindowContentId] listing.
    enums.settlementWindowStates.SETTLED,
    'All content aggregation records are SETTLED');
--1587319924704 - Update the [settlementWindowContentId] on [settlementWindowContent] with the ^^^ inserted id:
UPDATE settlementWindowContent SET settlementWindowContentId = ? 
WHERE settlementWindowContentId = ?;--from [settledContentIdList] variable. 
--1587319924705 - Update the [settlementWindow] for all settlements:
SELECT DISTINCT settlementWindowId FROM settlementWindowContent WHERE settlementWindowContentId IN ?;--settledContentIdList
--1587319924706 - Retrieve all the unsettled windows: 
SELECT DISTINCT swc.settlementWindowId FROM settlementWindowContent AS swc
JOIN settlementWindowContentStateChange AS swcsc ON swcsc.settlementWindowContentStateChangeId = swc.currentStateChangeId
WHERE swc.settlementWindowId IN ?--windowIdCheckList
AND
    swcsc.settlementWindowStateId != enums.settlementWindowStates.SETTLED;
--1587319924707 - Insert the settlementWindowStateChange for all the new settlements:
INSERT INTO settlementWindowStateChange (settlementWindowId, settlementWindowStateId, reason) VALUES(
    ?,--settlementWindowContentId from the [settlementWindowContentId] listing.
    enums.settlementWindowStates.SETTLED,
    'All settlement window content is SETTLED');
--1587319924708 - Update the [settlementWindowId] on [settlementWindow] with the ^^^ inserted id:
UPDATE settlementWindow SET currentStateChangeId = ?
WHERE settlementWindowId = ?;--from [settledWindowIdList] variable.
--1587319924709 - Obtain all the processed content:
SELECT DISTINCT sw.settlementWindowId, swsc.settlementWindowStateId, swsc.reason, sw.createdDate AS createdDate1, swsc.createdDate AS changedDate1,
swc.settlementWindowContentId, swcsc.settlementWindowStateId AS state, lat.name AS ledgerAccountType, swc.currencyId, swc.createdDate, 
swcsc.createdDate AS changedDate FROM settlementContentAggregation AS sca
JOIN settlementWindowContent AS swc ON swc.settlementWindowContentId = sca.settlementWindowContentId
JOIN settlementWindowContentStateChange AS swcsc ON swcsc.settlementWindowContentStateChangeId = swc.currentStateChangeId
JOIN ledgerAccountType AS lat ON lat.ledgerAccountTypeId = swc.ledgerAccountTypeId
JOIN settlementWindow AS sw ON sw.settlementWindowId = swc.settlementWindowId
JOIN settlementWindowStateChange AS swsc ON swsc.settlementWindowStateChangeId = sw.currentStateChangeId
WHERE sca.participantCurrencyId IN ?--settlementAccounts.changedIdList
AND sca.settlementId = ?--Current active settlement.
ORDER BY sw.settlementWindowId ASC, swc.settlementWindowContentId ASC;
--1587319924710 - In the event of the settlement state changed (due to state counters processed) the [settlementStateChange] needs to be updated:
INSERT INTO settlementStateChange (settlementStateId, reason) VALUES (
    ?,/*
        PS_TRANSFERS_RECORDED for PENDING_SETTLEMENT and pendingSettlementCount === 0
        PS_TRANSFERS_RESERVED for PS_TRANSFERS_RECORDED and psTransfersRecordedCount === 0
        PS_TRANSFERS_COMMITTED for PS_TRANSFERS_RESERVED and psTransfersReservedCount === 0
        SETTLING for PS_TRANSFERS_COMMITTED and psTransfersCommittedCount > 0 and settledCount > 0
        SETTLED for psTransfersCommittedCount === 0
        */
    ?/*
        PENDING_SETTLEMENT      -> 'All settlement accounts are PS_TRANSFERS_RECORDED'
        PS_TRANSFERS_RECORDED   -> 'All settlement accounts are PS_TRANSFERS_RESERVED'
        PS_TRANSFERS_RESERVED   -> 'All settlement accounts are PS_TRANSFERS_COMMITTED'
        PS_TRANSFERS_COMMITTED  -> 'Some settlement accounts are SETTLED' / 'All settlement accounts are SETTLED'
        
        */
)
--1587319924711 - Update [settlement] to point to the latest status:
UPDATE settlement SET currentStateChangeId = ?--settlementStateChangeId from the insert ^^^
WHERE settlementId = ?--settlementData.settlementId
```
