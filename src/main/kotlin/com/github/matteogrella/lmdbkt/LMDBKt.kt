/* Copyright 2019-present Matteo Grella. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * ------------------------------------------------------------------*/

package com.github.matteogrella.lmdbkt

import org.lmdbjava.Dbi
import org.lmdbjava.Env
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocateDirect
import org.lmdbjava.KeyRange
import org.lmdbjava.Env.create
import org.lmdbjava.DbiFlags.MDB_CREATE
import java.io.File
import java.nio.file.Path

/**
 * @param path the path to the database directory
 */
abstract class LMDBMap<K, V>(path: Path) : MutableMap<K, V>, Closeable {

    companion object {

        /**
         * Arbitrary DB name.
         */
        private const val DB_NAME = "__DB__"

        /**
         * @return a File object representing this path
         */
        private fun Path.toFileOrCreate(): File = this.toFile().also { if (!it.exists()) it.mkdir() }

        /**
         * Transform a ByteBuffer into a ByteArray.
         *
         * @return a byte array
         */
        private fun ByteBuffer.asByteArray(): ByteArray {
            this.rewind()
            return ByteArray(this.remaining()).also { this.get(it) }
        }
    }

    /**
     * The environment for a single db.
     */
    private val env: Env<ByteBuffer> = create()
      .setMapSize(1099511627776)
      .setMaxDbs(1)
      .open(path.toFileOrCreate())

    /**
     * The database.
     */
    private val db: Dbi<ByteBuffer> = this.env.openDbi(DB_NAME, MDB_CREATE)

    /**
     * The mutable entry of this map.
     *
     * @property key the key
     * @property values the value
     */
    inner class MutableEntry(override val key: K, override val value: V) : MutableMap.MutableEntry<K, V> {

        /**
         * Changes the value associated with the key of this entry.
         *
         * @return the previous value corresponding to the key.
         */
        override fun setValue(newValue: V): V = this@LMDBMap.put(this.key, newValue)!!
    }

    /**
     * @param obj the key
     *
     * @return the byte array representation of the [obj]
     */
    abstract fun serializeKey(obj: K): ByteArray

    /**
     * @param obj the value
     *
     * @return the byte array representation of the [obj]
     */
    abstract fun serializeValue(obj: V): ByteArray

    /**
     * @param obj the byte array representation of the key
     *
     * @return the key
     */
    abstract fun deserializeKey(obj: ByteArray): K

    /**
     * @param obj the byte array representation of the value
     *
     * @return the key
     */
    abstract fun deserializeValue(obj: ByteArray): V

    /**
     * Returns `true` if the map contains the specified [key].
     */
    override fun containsKey(key: K): Boolean = this[key] != null

    /**
     * Returns `true` if the map maps one or more keys to the specified [value].
     */
    override fun containsValue(value: V): Boolean {

        this.env.txnRead().use { txn ->
            this.db.iterate(txn, KeyRange.all<ByteBuffer>()).use {
                for (kv in it.iterable()) {

                    if (deserializeValue(kv.`val`().asByteArray()) == value) {
                        it.close()
                        txn.close()
                        return true
                    }
                }
            }
        }

        return false
    }

    /**
     * Returns the value corresponding to the given [key], or `null` if such a key is not present in the map.
     */
    override fun get(key: K): V? {

        val keyBuf = allocateDirect(this.env.maxKeySize)
        keyBuf.put(serializeKey(key)).flip()

        return this.env.txnRead().use { txn ->
            this.db.get(txn, keyBuf)?.let { this.deserializeValue(txn.`val`().asByteArray()) }
        }
    }

    /**
     * Returns `true` if the map is empty (contains no elements), `false` otherwise.
     */
    override fun isEmpty(): Boolean = this.size == 0

    /**
     * Returns a [MutableSet] of all key/value pairs in this map.
     */
    override val entries: MutableSet<MutableMap.MutableEntry<K, V>> get() {

        val entries = mutableSetOf<MutableMap.MutableEntry<K, V>>()

        this.env.txnRead().use { txn ->
            this.db.iterate(txn, KeyRange.all<ByteBuffer>()).use {
                for (kv in it.iterable()) {
                    val key = deserializeKey(kv.key().asByteArray())
                    val value = deserializeValue(kv.`val`().asByteArray())
                    entries.add(MutableEntry(key, value))
                }
            }
        }

        return entries
    }

    /**
     * @return the number of entries in this map
     */
    override val size: Int get() = this.env.txnRead().use { txn ->
        this.db.stat(txn).entries.toInt()
    }

    /**
     * Returns a [MutableSet] of all keys in this map.
     */
    override val keys: MutableSet<K> get() {

        val keys = mutableSetOf<K>()

        this.env.txnRead().use { txn ->
            this.db.iterate(txn, KeyRange.all<ByteBuffer>()).use {
                for (kv in it.iterable()) {
                    keys.add(deserializeKey(kv.key().asByteArray()))
                }
            }
        }

        return keys
    }

    /**
     * Returns a [MutableCollection] of all values in this map. Note that this collection may contain duplicate values.
     */
    override val values: MutableCollection<V> get() {

        val values = mutableListOf<V>()

        this.env.txnRead().use { txn ->
            this.db.iterate(txn, KeyRange.all<ByteBuffer>()).use {
                for (kv in it.iterable()) {
                    values.add(deserializeValue(kv.`val`().asByteArray()))
                }
            }
        }

        return values
    }

    /**
     * Removes all elements from this map.
     */
    override fun clear() {

        this.env.txnRead().use { txn ->
            this.db.iterate(txn, KeyRange.all<ByteBuffer>()).use {
                for (kv in it.iterable()) {
                    this.db.delete(kv.key())
                }
            }
        }
    }

    /**
     * Associates the specified [value] with the specified [key] in the map.
     *
     * @return the previous value associated with the key, or `null` if the key was not present in the map.
     */
    override fun put(key: K, value: V): V? {

        val prev: V? = this[key]

        val valueSer = serializeValue(value)
        val valueBuf = allocateDirect(valueSer.size)
        val keyBuf = allocateDirect(this.env.maxKeySize)

        keyBuf.put(serializeKey(key)).flip()
        valueBuf.put(valueSer).flip()

        this.db.put(keyBuf, valueBuf)

        return prev
    }

    /**
     * Updates this map with key/value pairs from the specified map [from].
     */
    override fun putAll(from: Map<out K, V>) {

        from.forEach { key, value ->
            this[key] = value
        }
    }

    /**
     * Removes the specified key and its corresponding value from this map.
     *
     * @return the previous value associated with the key, or `null` if the key was not present in the map.
     */
    override fun remove(key: K): V? {

        val prev: V? = this[key]

        val keyBuf = allocateDirect(this.env.maxKeySize)
        keyBuf.put(serializeKey(key)).flip()

        this.db.delete(keyBuf)

        return prev
    }

    /**
     * Close the DB.
     */
    override fun close() {
        this.db.close()
        this.env.close()
    }
}