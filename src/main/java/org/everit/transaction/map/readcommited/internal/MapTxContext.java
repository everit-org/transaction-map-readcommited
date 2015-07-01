/*
 * Copyright (C) 2011 Everit Kft. (http://www.everit.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.everit.transaction.map.readcommited.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * Stores the temporary changes of the Map that might be applied in the end of the transaciton.
 * Copied from 2-SNAPSHOT version of Apache Commons Transaction and modified.
 *
 * @param <K>
 *          Type of keys.
 * @param <V>
 *          Type of values.
 */
public class MapTxContext<K, V> implements Map<K, V> {

  protected boolean cleared;

  protected Map<K, V> puts;

  protected boolean readOnly = true;

  protected Set<K> removes;

  protected final RWLockedMap<K, V> rwLockedMap;

  protected final Object transaction;

  /**
   * Constructor.
   */
  public MapTxContext(final RWLockedMap<K, V> rwLockedMap, final Object transaction) {
    this.rwLockedMap = rwLockedMap;
    this.transaction = transaction;
    removes = new HashSet<K>();
    puts = new HashMap<K, V>();
    cleared = false;
  }

  @Override
  public void clear() {
    readOnly = false;
    cleared = true;
    removes.clear();
    puts.clear();
  }

  /**
   * Writes the temporary changes back to the Map.
   */
  public void commit() {
    if (isReadOnly()) {
      return;
    }

    Lock writeLock = rwLockedMap.getReadWriteLock().writeLock();
    writeLock.lock();
    try {
      if (cleared) {
        rwLockedMap.clear();
      } else {
        for (Object key : removes) {
          rwLockedMap.remove(key);
        }
      }

      rwLockedMap.putAll(puts);

    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean containsKey(final Object key) {
    if (removes.contains(key)) {
      // reflects that entry has been deleted in this tx
      return false;
    }

    if (puts.containsKey(key)) {
      return true;
    }

    if (cleared) {
      return false;
    } else {
      // not modified in this tx
      return rwLockedMap.containsKey(key);
    }
  }

  @Override
  public boolean containsValue(final Object value) {
    return values().contains(value);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Set<Entry<K, V>> entrySet = new HashSet<>();
    // XXX expensive :(
    Map<K, V> avoidFindbugsThis = this;
    for (K key : keySet()) {
      V value = avoidFindbugsThis.get(key);
      // XXX we have no isolation, so get entry might have been
      // deleted in the meantime
      if (value != null) {
        entrySet.add(new HashEntry<>(key, value));
      }
    }
    return Collections.unmodifiableSet(entrySet);
  }

  @Override
  public V get(final Object key) {

    if (removes.contains(key)) {
      // reflects that entry has been deleted in this tx
      return null;
    }

    if (puts.containsKey(key)) {
      return puts.get(key);
    }

    if (cleared) {
      return null;
    } else {
      // not modified in this tx
      return rwLockedMap.get(key);
    }
  }

  public Object getTransaction() {
    return transaction;
  }

  @Override
  public boolean isEmpty() {
    return (size() == 0);
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public Set<K> keySet() {
    Set<K> keySet = new HashSet<K>();
    if (!cleared) {
      keySet.addAll(rwLockedMap.keySet());
      keySet.removeAll(removes);
    }
    keySet.addAll(puts.keySet());
    return Collections.unmodifiableSet(keySet);
  }

  @Override
  public V put(final K key, final V value) {
    readOnly = false;

    V oldValue = get(key);

    puts.put(key, value);

    return oldValue;
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> map) {
    for (Object name : map.entrySet()) {
      @SuppressWarnings({ "rawtypes", "unchecked" })
      Map.Entry<K, V> entry = (Map.Entry) name;
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public V remove(final Object key) {
    V oldValue = get(key);

    readOnly = false;
    puts.remove(key);
    if (!cleared) {
      @SuppressWarnings("unchecked")
      K typedKey = (K) key;
      removes.add(typedKey);
    }

    return oldValue;
  }

  @Override
  public int size() {
    return keySet().size();
  }

  @Override
  public Collection<V> values() {
    // XXX expensive :(
    Collection<V> values = new ArrayList<V>();
    Set<K> keys = keySet();
    Map<K, V> avoidFindbugsThis = this;
    for (K key : keys) {
      V value = avoidFindbugsThis.get(key);
      // XXX we have no isolation, so entry might have been
      // deleted in the meantime
      if (value != null) {
        values.add(value);
      }
    }
    return Collections.unmodifiableCollection(values);
  }

}
