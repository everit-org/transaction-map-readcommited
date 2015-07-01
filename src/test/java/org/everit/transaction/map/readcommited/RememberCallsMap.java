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
package org.everit.transaction.map.readcommited;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

public class RememberCallsMap<K, V> implements Map<K, V> {

  private static class CallInfo {

    public final String methodName;

    public final Object[] parameters;

    public CallInfo(final String methodName, final Object... parameters) {
      this.methodName = methodName;
      this.parameters = parameters;
    }
  }

  private final LinkedList<CallInfo> calls = new LinkedList<>();

  private final Map<K, V> wrapped = new HashMap<>();

  @Override
  public void clear() {
    calls.addLast(new CallInfo("clear"));
    wrapped.clear();
  }

  @Override
  public boolean containsKey(final Object key) {
    return wrapped.containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    return wrapped.containsValue(value);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return wrapped.entrySet();
  }

  @Override
  public V get(final Object key) {
    return wrapped.get(key);
  }

  @Override
  public boolean isEmpty() {
    return wrapped.isEmpty();
  }

  @Override
  public Set<K> keySet() {
    return wrapped.keySet();
  }

  @Override
  public V put(final K key, final V value) {
    calls.addLast(new CallInfo("put", key, value));
    return wrapped.put(key, value);
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {
    calls.addLast(new CallInfo("putAll", m));
    wrapped.putAll(m);
  }

  @Override
  public V remove(final Object key) {
    calls.addLast(new CallInfo("remove", key));
    return wrapped.remove(key);
  }

  @Override
  public int size() {
    return wrapped.size();
  }

  @Override
  public Collection<V> values() {
    return wrapped.values();
  }

}
