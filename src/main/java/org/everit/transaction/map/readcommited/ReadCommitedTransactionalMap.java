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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.everit.transaction.map.TransactionalMap;
import org.everit.transaction.map.readcommited.internal.MapTxContext;
import org.everit.transaction.map.readcommited.internal.RWLockedMap;

/**
 * Transactional wrapper for {@link Map} interface that does all modification only during commiting
 * the transaction.
 *
 * @param <K>
 *          Type of the keys.
 * @param <V>
 *          Type of the values.
 */
public class ReadCommitedTransactionalMap<K, V> implements TransactionalMap<K, V> {

  protected ThreadLocal<MapTxContext<K, V>> activeTx = new ThreadLocal<>();

  protected final boolean allRemoveOperationReplayedOnCommit;

  protected final Map<Object, MapTxContext<K, V>> suspendedTXContexts = new ConcurrentHashMap<>();

  protected final RWLockedMap<K, V> wrapped;

  /**
   * Constructor.
   *
   * @param wrapped
   *          The Map that should is managed by this class.
   * @param allRemoveOperationReplayedOnCommit
   *          See {@link #isAllRemoveOperationReplayedOnCommit()}.
   */
  public ReadCommitedTransactionalMap(final Map<K, V> wrapped,
      final boolean allRemoveOperationReplayedOnCommit) {
    this.allRemoveOperationReplayedOnCommit = allRemoveOperationReplayedOnCommit;
    if (wrapped != null) {
      this.wrapped = new RWLockedMap<>(wrapped);
    } else {
      this.wrapped = new RWLockedMap<>(new HashMap<>());
    }
  }

  @Override
  public void clear() {
    coalesceActiveTxOrWrapped().clear();
  }

  protected Map<K, V> coalesceActiveTxOrWrapped() {
    MapTxContext<K, V> txContext = getActiveTx();
    return (txContext != null) ? txContext : wrapped;
  }

  @Override
  public void commitTransaction() {
    getActiveTx().commit();
    setActiveTx(null);
  }

  @Override
  public boolean containsKey(final Object key) {
    return coalesceActiveTxOrWrapped().containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    return coalesceActiveTxOrWrapped().containsValue(value);
  }

  protected MapTxContext<K, V> createMapTxContext(final Object transaction) {
    return new MapTxContext<K, V>(wrapped, transaction, allRemoveOperationReplayedOnCommit);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return coalesceActiveTxOrWrapped().entrySet();
  }

  @Override
  public V get(final Object key) {
    return coalesceActiveTxOrWrapped().get(key);
  }

  protected MapTxContext<K, V> getActiveTx() {
    return activeTx.get();
  }

  @Override
  public Object getAssociatedTransaction() {
    return getActiveTx().getTransaction();
  }

  @Override
  public boolean isAllRemoveOperationReplayedOnCommit() {
    return allRemoveOperationReplayedOnCommit;
  }

  @Override
  public boolean isEmpty() {
    return coalesceActiveTxOrWrapped().isEmpty();
  }

  @Override
  public Set<K> keySet() {
    return coalesceActiveTxOrWrapped().keySet();
  }

  @Override
  public V put(final K key, final V value) {
    return coalesceActiveTxOrWrapped().put(key, value);
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {
    coalesceActiveTxOrWrapped().putAll(m);
  }

  @Override
  public V remove(final Object key) {
    return coalesceActiveTxOrWrapped().remove(key);
  }

  /**
   * Resumes a transaction that was previously suspended.
   *
   * @param transaction
   *          The transaction to resume.
   * @throws NullPointerException
   *           if there is no suspended transaction registered.
   */
  @Override
  public void resumeTransaction(final Object transaction) {
    MapTxContext<K, V> txContext = suspendedTXContexts.remove(transaction);
    Objects.requireNonNull(txContext);
    setActiveTx(txContext);
  }

  @Override
  public void rollbackTransaction() {
    setActiveTx(null);
  }

  protected void setActiveTx(final MapTxContext<K, V> mapContext) {
    activeTx.set(mapContext);
  }

  @Override
  public int size() {
    return coalesceActiveTxOrWrapped().size();
  }

  @Override
  public void startTransaction(final Object transaction) {
    setActiveTx(createMapTxContext(transaction));
  }

  /**
   * Suspends the context for a specific transaction.
   *
   */
  @Override
  public void suspendTransaction() {
    MapTxContext<K, V> activeTx = getActiveTx();
    Objects.requireNonNull(activeTx, "No active map context for transaction.");
    suspendedTXContexts.put(activeTx.getTransaction(), activeTx);
    setActiveTx(null);
  }

  @Override
  public Collection<V> values() {
    return coalesceActiveTxOrWrapped().values();
  }

}
