package org.everit.transaction.map.readcommited.internal;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Mostly copied from org.apache.commons.collections.map.AbstractHashedMap.
 *
 * @param <K>
 *          The type of the key of the Map.
 * @param <V>
 *          The type of the value of the Map.
 */
public class HashEntry<K, V> implements Entry<K, V> {

  protected K key;

  protected V value;

  public HashEntry(final K key, final V value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof Map.Entry)) {
      return false;
    }
    @SuppressWarnings("unchecked")
    Map.Entry<K, V> other = (Map.Entry<K, V>) obj;
    return (getKey() == null ? other.getKey() == null : getKey().equals(other.getKey()))
        && (getValue() == null ? other.getValue() == null : getValue().equals(
            other.getValue()));
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return (getKey() == null ? 0 : getKey().hashCode())
        ^ (getValue() == null ? 0 : getValue().hashCode());
  }

  @Override
  public V setValue(final V value) {
    V old = this.value;
    this.value = value;
    return old;
  }

  @Override
  public String toString() {
    return new StringBuffer().append(getKey()).append('=').append(getValue()).toString();
  }
}
