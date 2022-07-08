# Mojaloop - Central Services
Business logic operations for Mojaloop `central-ledger` and `central-settlement`.

## Central-Ledger
MJL service responsible for clearing.

### Prepare-Position
Phase-1/2 of the 2-phase transfer.

Prepare duplicate check:
```sql
--1587309924652
insert into `transferDuplicateCheck` (`hash`, `transferId`) values (?, ?)
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
insert into `transfer` (`amount`, `currencyId`, `expirationDate`, `ilpCondition`, `transferId`) values (?, ?, ?, ?, ?)
--1587309924656 
insert into `transferParticipant` (`amount`, `ledgerEntryTypeId`, `participantCurrencyId`, `transferId`, `transferParticipantRoleTypeId`) values (?, ?, ?, ?, ?)
--1587309924656
insert into `transferParticipant` (`amount`, `ledgerEntryTypeId`, `participantCurrencyId`, `transferId`, `transferParticipantRoleTypeId`) values (?, ?, ?, ?, ?)
--1587309924656 (Inserted as part of a batch)
insert into `transferExtension` (`transferId`, `key`, `value`) values (?, ?, ?)
--1587309924656
insert into `ilpPacket` (`transferId`, `value`) values (?, ?)
--1587309924658
insert into `transferStateChange` (`createdDate`, `reason`, `transferId`, `transferStateId`) values (?, ?, ?, ?)
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
select `transfer`.*, `transfer`.`currencyId` as `currency`, `pc1`.`participantCurrencyId` as `payerParticipantCurrencyId`, `tp1`.`amount` as `payerAmount`, `da`.`participantId` as `payerParticipantId`, `da`.`name` as `payerFsp`, `pc2`.`participantCurrencyId` as `payeeParticipantCurrencyId`, `tp2`.`amount` as `payeeAmount`, `ca`.`participantId` as `payeeParticipantId`, `ca`.`name` as `payeeFsp`, `tsc`.`transferStateChangeId`, `tsc`.`transferStateId` as `transferState`, `tsc`.`reason` as `reason`, `tsc`.`createdDate` as `completedTimestamp`, `ts`.`enumeration` as `transferStateEnumeration`, `ts`.`description` as `transferStateDescription`, `ilpp`.`value` as `ilpPacket`, `transfer`.`ilpCondition` as `condition`, `tf`.`ilpFulfilment` as `fulfilment`, `te`.`errorCode`, `te`.`errorDescription` 
from `transfer` 
inner join `transferParticipant` as `tp1` on `tp1`.`transferId` = `transfer`.`transferId` 
inner join `transferParticipantRoleType` as `tprt1` on `tprt1`.`transferParticipantRoleTypeId` = `tp1`.`transferParticipantRoleTypeId` 
inner join `participantCurrency` as `pc1` on `pc1`.`participantCurrencyId` = `tp1`.`participantCurrencyId` 
inner join `participant` as `da` on `da`.`participantId` = `pc1`.`participantId` 
inner join `transferParticipant` as `tp2` on `tp2`.`transferId` = `transfer`.`transferId` 
inner join `transferParticipantRoleType` as `tprt2` on `tprt2`.`transferParticipantRoleTypeId` = `tp2`.`transferParticipantRoleTypeId` 
inner join `participantCurrency` as `pc2` on `pc2`.`participantCurrencyId` = `tp2`.`participantCurrencyId` 
inner join `participant` as `ca` on `ca`.`participantId` = `pc2`.`participantId` 
inner join `ilpPacket` as `ilpp` on `ilpp`.`transferId` = `transfer`.`transferId` 
left join `transferStateChange` as `tsc` on `tsc`.`transferId` = `transfer`.`transferId` 
left join `transferState` as `ts` on `ts`.`transferStateId` = `tsc`.`transferStateId` 
left join `transferFulfilment` as `tf` on `tf`.`transferId` = `transfer`.`transferId` 
left join `transferError` as `te` on `te`.`transferId` = `transfer`.`transferId` 
where `transfer`.`transferId` = ? and `tprt1`.`name` = ? and `tprt2`.`name` = ? and pc1.currencyId = transfer.currencyId and pc2.currencyId = transfer.currencyId 
order by `tsc`.`transferStateChangeId` desc limit ?
--1587309924757 
select * from `transferExtension` where `transferId` = ? and `isFulfilment` = ? and `isError` = ?
--1587309924759 
insert into `transferFulfilmentDuplicateCheck` (`hash`, `transferId`) values (?, ?)
--1587309924768 
select `transferParticipant`.*, `tsc`.`transferStateId`, `tsc`.`reason` from `transferParticipant` inner join `transferStateChange` as `tsc` on `tsc`.`transferId` = `transferParticipant`.`transferId` where `transferParticipant`.`transferId` = ? and `transferParticipant`.`transferParticipantRoleTypeId` = ? and `transferParticipant`.`ledgerEntryTypeId` = ? order by `tsc`.`transferStateChangeId` desc limit ?
--1587309924770 
select `settlementWindow`.`settlementWindowId`, `swsc`.`settlementWindowStateId` as `state`, `swsc`.`reason` as `reason`, `settlementWindow`.`createdDate` as `createdDate`, `swsc`.`createdDate` as `changedDate` 
from `settlementWindow` left join `settlementWindowStateChange` as `swsc` on `swsc`.`settlementWindowStateChangeId` = `settlementWindow`.`currentStateChangeId` where `swsc`.`settlementWindowStateId` = ? order by `changedDate` desc
--1587309924771 
insert into `transferFulfilment` (`completedDate`, `createdDate`, `ilpFulfilment`, `isValid`, `settlementWindowId`, `transferId`) values (?, ?, ?, ?, ?, ?)
--1587309924774 
insert into `transferStateChange` (`createdDate`, `transferId`, `transferStateId`) values (?, ?, ?)
--1587309924775 
UPDATE participantPosition SET value = (value + -100), changedDate = '2020-04-19 15:25:24.769' WHERE participantPositionId = 5
--1587309924775 
select * from `transferStateChange` where `transferId` = ? order by `transferStateChangeId` desc limit ? for update
--1587309924776 
INSERT INTO participantPositionChange (participantPositionId, transferStateChangeId, value, reservedValue, createdDate) SELECT 5, 1254, value, reservedValue, '2020-04-19 15:25:24.769' FROM participantPosition WHERE participantPositionId = 5
```

Validate:
```javascript
if (payload.fulfilment && !Validator.validateFulfilCondition(payload.fulfilment, transfer.condition))
if (transfer.transferState !== TransferState.RESERVED)
if (transfer.expirationDate <= new Date(Util.Time.getUTCString(new Date())))
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
Initiate the settlement for all applicable settlement models.

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
select * from `settlementModel` where `name` = ? and isActive = 1
--1587309924789  - Get all settlement windows by settlement models:
SELECT * FROM `settlementWindow` 
left join settlementWindowStateChange AS swsc ON swsc.settlementWindowStateChangeId = settlementWindow.currentStateChangeId 
left join settlementWindowContent AS swc ON swc.settlementWindowId = settlementWindow.settlementWindowId 
left join settlementWindowContentStateChange AS swcsc ON swcsc.settlementWindowContentStateChangeId = swc.currentStateChangeId 
where settlementWindow.settlementWindowId IN ? 
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
left join settlementWindowStateChange AS swsc ON swsc.settlementWindowStateChangeId = settlementWindow.currentStateChangeId 
left join settlementWindowContent AS swc ON swc.settlementWindowId = settlementWindow.settlementWindowId 
left join settlementWindowContentStateChange AS swcsc ON swcsc.settlementWindowContentStateChangeId = swc.currentStateChangeId 
where settlementWindow.settlementWindowId IN ? 
and ledgerAccountTypeId = ? 
and swc.currencyId = ?
and swsc.settlementWindowStateId = winStateEnum.CLOSED, winStateEnum.ABORTED, winStateEnum.PENDING_SETTLEMENT
and swcsc.settlementWindowStateId = [winStateEnum.CLOSED, winStateEnum.ABORTED]
--1587309924793 - Bind requested settlementWindowContent and settlementContentAggregation records
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
--1587309924801 - Set state of CLOSED and ABORTED windows to PENDING_SETTLEMENT, skip already in PENDING_SETTLEMENT state
SELECT sw.settlementWindowId FROM settlementWindow AS sw 
left join settlementWindowStateChange AS swsc ON swsc.settlementWindowStateChangeId = sw.currentStateChangeId
WHERE sw.settlementWindowId = ? and swsc.settlementWindowStateId IN (enums.settlementWindowStates.CLOSED, enums.settlementWindowStates.ABORTED)
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

### Settlement Event - Record Transfers
Initiate the settlement transactions.
`TODO`

### Settlement Event - Reservation
Process the settlement reservation for payee.
`TODO`

### Settlement Event - Commit
Process the settlement commit for payer.
`TODO`


