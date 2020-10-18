const HashTable = require('@ronomon/hash-table');
const assert = require('assert');

const runs = 5;
const insertions = 1000000;
const key = Buffer.alloc(16);
const transfer = Buffer.alloc(128);

const keySize = key.length;
const valueSize = transfer.length;
const elementsMin = insertions * runs;
const elementsMax = elementsMin;

const transfers = new HashTable(keySize, valueSize, elementsMin, elementsMax);

var id = 0;
for (var run = 0; run < runs; run++) {
  var start = Date.now();
  for (var count = 0; count < insertions; count++) {
    key.writeUInt32LE(id++, 0);
    if (transfers.exist(key, 0)) {
      throw new Error('transfer exists!');
    } else {
      transfers.set(key, 0, transfer, 0);
    }
  }
  console.log(`${insertions} hash table insertions in ${Date.now() - start}ms`);
}
