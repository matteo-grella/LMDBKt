package com.github.matteogrella.lmdbkt

import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.Test
import java.nio.file.Files

class LMDBMapTest {

  private fun openLMDBMap(): LMDBMap<String, String> {

    return object : LMDBMap<String, String>(
      Files.createTempDirectory("lmdbmap-test")
    ) {
      override fun serializeKey(obj: String): ByteArray = obj.toByteArray()
      override fun serializeValue(obj: String): ByteArray = obj.toByteArray()
      override fun deserializeKey(obj: ByteArray): String = String(obj)
      override fun deserializeValue(obj: ByteArray): String = String(obj)
    }
  }

  @Test
  fun testEmptyMapSize() {
    openLMDBMap().use { dbMap ->
      assertThat(dbMap.size == 0).isTrue()
    }
  }

  @Test
  fun testIsEmpty() {
    openLMDBMap().use { dbMap ->
      assertThat(dbMap.isEmpty()).isTrue()
    }
  }

  @Test
  fun testContainsKey() {
    openLMDBMap().use { dbMap ->

      dbMap.putAll(mapOf(
        "key1" to "value1",
        "key2" to "value2",
        "key3" to "value3",
        "key4" to "value3" // same value as key3
      ))

      assertThat(dbMap.containsKey("key1")).isTrue()
      assertThat(dbMap.containsKey("key2")).isTrue()
      assertThat(dbMap.containsKey("key3")).isTrue()
      assertThat(dbMap.containsKey("key4")).isTrue()
      assertThat(dbMap.containsKey("key_that_doesn't_exist")).isFalse()
    }
  }

  @Test
  fun testContainsValue() {
    openLMDBMap().use { dbMap ->

      dbMap.putAll(mapOf(
        "key1" to "value1",
        "key2" to "value2",
        "key3" to "value3",
        "key4" to "value3" // same value as key3
      ))

      assertThat(dbMap.containsValue("value3")).isTrue()
      assertThat(dbMap.containsValue("value_that_doesn't_exist")).isFalse()
    }
  }

  @Test
  fun testGet() {
    openLMDBMap().use { dbMap ->

      dbMap.putAll(mapOf(
        "key1" to "value1",
        "key2" to "value2",
        "key3" to "value3",
        "key4" to "value3" // same value as key3
      ))

      assertThat(dbMap["key1"] == "value1").isTrue()
      assertThat(dbMap["key2"] == "value2").isTrue()
      assertThat(dbMap["key3"] == "value3").isTrue()
      assertThat(dbMap["key4"] == "value3").isTrue()
      assertThat(dbMap["key5"] == null).isTrue()
    }
  }

  @Test
  fun testSize() {
    openLMDBMap().use { dbMap ->

      dbMap.putAll(mapOf(
        "key1" to "value1",
        "key2" to "value2",
        "key3" to "value3",
        "key4" to "value3"
      ))

      assertThat(dbMap.size == 4).isTrue()
    }
  }

  @Test
  fun testRemove() {
    openLMDBMap().use { dbMap ->

      dbMap.putAll(mapOf(
        "key1" to "value1",
        "key2" to "value2",
        "key3" to "value3",
        "key4" to "value3" // same value as key3
      ))

      assertThat(dbMap.remove("key1")).isEqualTo("value1")
      assertThat(dbMap.remove("key1")).isEqualTo(null)
    }
  }

  @Test
  fun testContainsKeyAfterRemove() {
    openLMDBMap().use { dbMap ->

      dbMap.putAll(mapOf(
        "key1" to "value1",
        "key2" to "value2",
        "key3" to "value3",
        "key4" to "value3" // same value as key3
      ))

      dbMap.remove("key2")
      dbMap.remove("key5") // no effect

      assertThat(dbMap.containsKey("key1")).isTrue()
      assertThat(dbMap.containsKey("key2")).isFalse()
      assertThat(dbMap.containsKey("key3")).isTrue()
      assertThat(dbMap.containsKey("key4")).isTrue()
      assertThat(dbMap.containsKey("key_that_doesn't_exist")).isFalse()
    }
  }

  @Test
  fun testIsEmptyAfterRemove() {
    openLMDBMap().use { dbMap ->

      dbMap.putAll(mapOf(
        "key1" to "value1",
        "key2" to "value2"
      ))

      dbMap.remove("key1")
      dbMap.remove("key2")

      assertThat(dbMap.isEmpty()).isTrue()
    }
  }

  @Test
  fun testIsEmptyAfterClear() {
    openLMDBMap().use { dbMap ->

      dbMap.putAll(mapOf(
        "key1" to "value1",
        "key2" to "value2"
      ))

      dbMap.clear()

      assertThat(dbMap.isEmpty()).isTrue()
    }
  }

  @Test
  fun testGetKeys() {
    openLMDBMap().use { dbMap ->

      dbMap.putAll(mapOf(
        "key1" to "value1",
        "key2" to "value2",
        "key3" to "value3",
        "key4" to "value3" // same value as key3
      ))

      assertThat(dbMap.keys == setOf("key1", "key2", "key3", "key4")).isTrue()
    }
  }

  @Test
  fun testGetValues() {
    openLMDBMap().use { dbMap ->

      dbMap.putAll(mapOf(
        "key1" to "value1",
        "key2" to "value2",
        "key3" to "value3",
        "key4" to "value3" // same value as key3
      ))

      assertThat(dbMap.values == listOf("value1", "value2", "value3", "value3")).isTrue()
    }
  }

  @Test
  fun testGetEntries() {
    openLMDBMap().use { dbMap ->

      val content = mapOf(
        "key1" to "value1",
        "key2" to "value2",
        "key3" to "value3",
        "key4" to "value3" // same value as key3
      )

      dbMap.putAll(content)

      val entries = dbMap.entries

      assertThat(entries.size == 4).isTrue()
      assertThat(entries.associate { Pair(it.key, it.value) } == content).isTrue()
    }
  }

  @Test
  fun testModifyEntries() {
    openLMDBMap().use { dbMap ->

      dbMap.putAll(mapOf(
        "key1" to "value1",
        "key2" to "value2",
        "key3" to "value3",
        "key4" to "value3" // same value as key3
      ))

      dbMap.entries.find { it.key == "key4" }!!.setValue("value4")

      assertThat(dbMap["key4"] == "value4").isTrue()
    }
  }

}
