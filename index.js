'use strict';

let crypto = require('crypto');
let path = require('path');
let testDirPath = path.resolve(__dirname, './benchdata');

let rimraf = require('rimraf');
let mkdirp = require('mkdirp');
let benchmark = require('benchmark');
let suite = new benchmark.Suite();

let lmdb = require('node-lmdb');

let env;
let dbi;
let keys = [];
let total = 1000000;

function cleanup(done) {
    // cleanup previous test directory
    rimraf(testDirPath, function (err) {
        if (err) {
            return done(err);
        }
        // setup clean directory
        mkdirp(testDirPath, function (err) {
            if (err) {
                return done(err);
            }
            done();
        });
    });
}

function setup() {
    env = new lmdb.Env();
    env.open({
        path: testDirPath,
        maxDbs: 10,
        mapSize: 16 * 1024 * 1024 * 1024
    });
    dbi = env.openDbi({
        name: 'benchmarks',
        create: true
    });

    let txn = env.beginTxn();
    let c = 0;
    while (c < total) {
        let key = new Buffer(new Array(8));
        key.writeDoubleBE(c);
        keys.push(key.toString('hex'));
        txn.putBinary(dbi, key.toString('hex'), crypto.randomBytes(32));
        c++;
    }
    txn.commit();
}

let txn;
let c = 0;

function randomizeIndex() {
    c = Math.random() % total;
}

function getIndex() {
    if (c < total - 1) {
        c++;
    } else {
        c = 0;
    }
    return c;
}

function getBinary() {
    let data = txn.getBinary(dbi, keys[getIndex()]);
}

function getBinaryUnsafe() {
    let data = txn.getBinaryUnsafe(dbi, keys[getIndex()]);
}

function getString() {
    let data = txn.getString(dbi, keys[getIndex()]);
}

function getStringUnsafe() {
    let data = txn.getStringUnsafe(dbi, keys[getIndex()]);
}

let cursor;

function cursorGoToNext() {
    let readed = 0;

    return () => {
        let c = cursor.goToNext();
        readed++;
        if (readed >= total) {
            cursor.goToRange(keys[0]);
            readed = 0; // reset to prevent goToRange on every loop
        }
    }
}

function cursorGoToNextgetCurrentBinary() {
    let readed = 0;

    return () => {
        const c = cursor.goToNext();
        readed++;
        if (readed >= total) {
            cursor.goToRange(keys[0]);
            readed = 0; // reset to prevent goToRange on every loop
        }
        const v = cursor.getCurrentBinary();
    }
}

cleanup(function (err) {
    if (err) {
        throw err;
    }

    setup();

    suite.add('getBinary', getBinary);
    suite.add('getBinaryUnsafe', getBinaryUnsafe);
    suite.add('getString', getString);
    suite.add('getStringUnsafe', getStringUnsafe);
    suite.add('cursorGoToNext', cursorGoToNext());
    suite.add('cursorGoToNextgetCurrentBinary', cursorGoToNextgetCurrentBinary());

    suite.on('start', function () {
        txn = env.beginTxn();
    });

    suite.on('cycle', function (event) {
        txn.abort();
        txn = env.beginTxn();
        if (cursor) cursor.close();
        cursor = new lmdb.Cursor(txn, dbi, {keyIsBuffer: true});
        console.log(String(event.target));
    });

    suite.on('complete', function () {
        txn.abort();
        dbi.close();
        env.close();
        if (cursor)
            cursor.close();
        console.log('Fastest is ' + this.filter('fastest').map('name'));
    });

    suite.run();

});
