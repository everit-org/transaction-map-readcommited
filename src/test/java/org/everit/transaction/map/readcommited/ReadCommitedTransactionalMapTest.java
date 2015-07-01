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
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.everit.transaction.map.readcommited.RememberManipulationCallsMap.CallInfo;
import org.junit.Assert;
import org.junit.Test;

public class ReadCommitedTransactionalMapTest {

  private void assertEntrySetKeySetSizeContainsXAndValue(
      final ReadCommitedTransactionalMap<String, String> map) {
    Set<Entry<String, String>> entrySet = map.entrySet();
    Set<String> keySet = map.keySet();
    Collection<String> values = map.values();

    Assert.assertEquals(2, map.size());
    Assert.assertEquals(2, entrySet.size());
    Assert.assertEquals(2, keySet.size());
    Assert.assertEquals(2, values.size());

    Assert.assertTrue(map.containsKey("outsideTransactionKey"));
    Assert.assertTrue(map.containsValue("outsideTransactionValue"));
    Assert.assertTrue(entrySetContains(entrySet, "outsideTransactionKey",
        "outsideTransactionValue"));
    Assert.assertTrue(keySet.contains("outsideTransactionKey"));
    Assert.assertTrue(values.contains("outsideTransactionValue"));

    Assert.assertFalse(map.containsKey("outsideTransactionKeyWithRemoveInTransaction"));
    Assert.assertFalse(map.containsValue("outsideTransactionValueWithRemoveInTransaction"));
    Assert.assertFalse(entrySetContains(entrySet, "outsideTransactionKeyWithRemoveInTransaction",
        "outsideTransactionValueWithRemoveInTransaction"));
    Assert.assertFalse(keySet.contains("outsideTransactionKeyWithRemoveInTransaction"));
    Assert.assertFalse(values.contains("outsideTransactionKeyWithRemoveInTransaction"));

    Assert.assertTrue(map.containsKey("insideTransactionKey"));
    Assert.assertTrue(map.containsValue("insideTransactionValue"));
    Assert.assertTrue(entrySetContains(entrySet, "insideTransactionKey",
        "insideTransactionValue"));
    Assert.assertTrue(keySet.contains("insideTransactionKey"));
    Assert.assertTrue(values.contains("insideTransactionValue"));
  }

  private <T extends Throwable> void callWithExpectedException(final Class<T> throwableType,
      final Runnable action) {
    try {
      action.run();
      Assert.fail("Exception should have been thrown");
    } catch (Throwable t) {
      Assert.assertTrue(throwableType.isAssignableFrom(t.getClass()));
    }
  }

  private boolean entrySetContains(final Set<Entry<String, String>> entrySet, final String key,
      final String value) {

    for (Entry<String, String> entry : entrySet) {
      if (key.equals(entry.getKey()) && value.equals(entry.getValue())) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testAllRemoveShouldBeCalled() {
    RememberManipulationCallsMap<String, String> wrapped = new RememberManipulationCallsMap<>();
    ReadCommitedTransactionalMap<String, String> transactionalMap =
        new ReadCommitedTransactionalMap<>(wrapped);

    transactionalMap.startTransaction("transactionOne");
    transactionalMap.remove("onlyRemovedKey");
    transactionalMap.put("putAndRemoveAndPutKey", "value");
    transactionalMap.remove("putAndRemoveAndPutKey");
    transactionalMap.put("putAndRemoveAndPutKey", "value");
    transactionalMap.remove("putAndRemoveAndPutKey");
    transactionalMap.put("putAndRemoveAndPutKey", "value");

    Assert.assertNull(wrapped.peekCall());
    transactionalMap.commitTransaction();

    Set<String> calledRemoves = new HashSet<>();
    CallInfo call = wrapped.peekCall();
    Assert.assertEquals("remove", call.methodName);
    calledRemoves.add((String) call.parameters[0]);
    call = wrapped.peekCall();

    Assert.assertEquals("remove", call.methodName);
    calledRemoves.add((String) call.parameters[0]);
    call = wrapped.peekCall();

    Assert.assertTrue(calledRemoves.contains("onlyRemovedKey"));
    Assert.assertTrue(calledRemoves.contains("putAndRemoveAndPutKey"));

    Assert.assertEquals("put", call.methodName);
    call = wrapped.peekCall();
    Assert.assertNull(call);
  }

  @Test
  public void testClear() {
    RememberManipulationCallsMap<String, String> wrapped = new RememberManipulationCallsMap<>();
    ReadCommitedTransactionalMap<String, String> transactionalMap =
        new ReadCommitedTransactionalMap<>(wrapped);

    transactionalMap.put("clearedKey", "value");
    wrapped.peekCall();

    transactionalMap.startTransaction(0);

    transactionalMap.put("putAndRemovedBeforeClear", "value");
    transactionalMap.remove("putAndRemovedBeforeClear");
    transactionalMap.clear();

    Assert.assertTrue(transactionalMap.isEmpty());
    Assert.assertEquals(0, transactionalMap.size());
    Assert.assertFalse(transactionalMap.containsKey("clearedKey"));
    Assert.assertFalse(transactionalMap.containsKey("putAndRemovedBeforeClear"));

    transactionalMap.remove("removeAndPutAfterClear");
    transactionalMap.put("removeAndPutAfterClear", "value");

    transactionalMap.put("putAndRemoveAfterClear", "value");
    transactionalMap.remove("putAndRemoveAfterClear");

    transactionalMap.commitTransaction();

    CallInfo callInfo = wrapped.peekCall();
    Assert.assertEquals("clear", callInfo.methodName);

    callInfo = wrapped.peekCall();
    Assert.assertEquals("put", callInfo.methodName);
    Assert.assertEquals("removeAndPutAfterClear", callInfo.parameters[0]);

    callInfo = wrapped.peekCall();
    Assert.assertNull(callInfo);
  }

  @Test
  public void testCommitWithoutAnyChange() {
    ReadCommitedTransactionalMap<String, String> transactionalMap =
        new ReadCommitedTransactionalMap<>(null);

    transactionalMap.put("key", "value");
    transactionalMap.startTransaction(0);
    transactionalMap.commitTransaction();
    Assert.assertEquals(1, transactionalMap.size());
  }

  @Test
  public void testEntrySetKeySetSizeContainsXAndValues() {
    ReadCommitedTransactionalMap<String, String> map =
        new ReadCommitedTransactionalMap<>(null);

    map.put("outsideTransactionKey", "outsideTransactionValue");
    map.put("outsideTransactionKeyWithRemoveInTransaction",
        "outsideTransactionValueWithRemoveInTransaction");

    map.startTransaction(0);
    map.put("insideTransactionKey", "insideTransactionValue");
    map.remove("outsideTransactionKeyWithRemoveInTransaction");

    assertEntrySetKeySetSizeContainsXAndValue(map);

    map.commitTransaction();
    assertEntrySetKeySetSizeContainsXAndValue(map);
  }

  @Test
  public void testIllegalCommitWithNoSuspendedTransaction() {
    callWithExpectedException(IllegalStateException.class,
        () -> new ReadCommitedTransactionalMap<>(null).commitTransaction());
  }

  @Test
  public void testIllegalResumeWithNoSuspendedTransaction() {
    callWithExpectedException(IllegalStateException.class,
        () -> new ReadCommitedTransactionalMap<>(null).resumeTransaction(0));
  }

  @Test
  public void testIllegalStateResumeWithOngoingTransaction() {
    ReadCommitedTransactionalMap<Object, Object> map = new ReadCommitedTransactionalMap<>(null);
    map.startTransaction(0);
    map.suspendTransaction();
    map.startTransaction(1);
    callWithExpectedException(IllegalStateException.class,
        () -> map.resumeTransaction(0));

    callWithExpectedException(IllegalStateException.class,
        () -> map.resumeTransaction(1));
  }

  @Test
  public void testIllegalStateRollbackWithNoTransaction() {
    callWithExpectedException(IllegalStateException.class,
        () -> new ReadCommitedTransactionalMap<>(null).rollbackTransaction());

    ReadCommitedTransactionalMap<Object, Object> map = new ReadCommitedTransactionalMap<>(null);
    map.startTransaction(0);
    map.suspendTransaction();
    callWithExpectedException(IllegalStateException.class,
        () -> map.rollbackTransaction());
  }

  @Test
  public void testIllegalStateStartSuspendedTransaction() {
    ReadCommitedTransactionalMap<Object, Object> map = new ReadCommitedTransactionalMap<>(null);
    map.startTransaction(0);
    map.suspendTransaction();
    callWithExpectedException(IllegalStateException.class,
        () -> map.startTransaction(0));
  }

  @Test
  public void testIllegalStateStartWithOngoingTransaction() {
    ReadCommitedTransactionalMap<Object, Object> map = new ReadCommitedTransactionalMap<>(null);
    map.startTransaction(0);
    callWithExpectedException(IllegalStateException.class,
        () -> map.startTransaction(0));

    callWithExpectedException(IllegalStateException.class,
        () -> map.startTransaction(1));
  }

  @Test
  public void testIllegalStateSuspendWithNoTransaction() {
    callWithExpectedException(IllegalStateException.class,
        () -> new ReadCommitedTransactionalMap<>(null).suspendTransaction());
  }

  @Test
  public void testPutAll() {
    ReadCommitedTransactionalMap<String, String> transactionalMap =
        new ReadCommitedTransactionalMap<>(null);

    Map<String, String> valuesToPut = new HashMap<>();
    valuesToPut.put("key1", "value");
    valuesToPut.put("key2", "value");
    transactionalMap.putAll(valuesToPut);
    Assert.assertEquals(2, transactionalMap.size());

    valuesToPut.remove("key1");
    valuesToPut.put("key3", "value");

    transactionalMap.startTransaction(0);
    transactionalMap.putAll(valuesToPut);
    Assert.assertEquals(3, transactionalMap.size());
    transactionalMap.commitTransaction();
    Assert.assertEquals(3, transactionalMap.size());
  }

  @Test
  public void testRollback() {
    RememberManipulationCallsMap<String, String> wrapped = new RememberManipulationCallsMap<>();
    ReadCommitedTransactionalMap<String, String> transactionalMap =
        new ReadCommitedTransactionalMap<>(wrapped);

    transactionalMap.put("outsideTransactionKey", "value");
    transactionalMap.startTransaction(0);
    transactionalMap.clear();
    transactionalMap.put("insideTransactionKey", "value");
    transactionalMap.rollbackTransaction();

    Assert.assertEquals(1, transactionalMap.size());
    Assert.assertTrue(transactionalMap.containsKey("outsideTransactionKey"));
    Assert.assertFalse(transactionalMap.containsKey("insideTransactionKey"));
  }

  @Test
  public void testSuspendAndResume() {
    RememberManipulationCallsMap<String, String> wrapped = new RememberManipulationCallsMap<>();
    ReadCommitedTransactionalMap<String, String> transactionalMap =
        new ReadCommitedTransactionalMap<>(wrapped);

    transactionalMap.put("keyOutsideTransaction_1", "value");
    wrapped.peekCall();

    transactionalMap.startTransaction(0);
    transactionalMap.put("keyWithinTransaction0", "value");
    transactionalMap.put("keyWithinBothTransaction", "value0");

    Assert.assertEquals(0, transactionalMap.getAssociatedTransaction());
    transactionalMap.suspendTransaction();

    Assert.assertEquals(null, transactionalMap.getAssociatedTransaction());
    Assert.assertEquals(1, transactionalMap.size());
    Assert.assertFalse(transactionalMap.containsKey("keyWithinTransaction0"));

    Assert.assertNull(wrapped.peekCall());

    transactionalMap.startTransaction(1);
    transactionalMap.put("keyWithinTransaction1", "value");
    transactionalMap.put("keyWithinBothTransaction", "value1");
    transactionalMap.suspendTransaction();

    transactionalMap.resumeTransaction(0);
    transactionalMap.put("keyAfterResumeTransaction0ButDeletedInTransaction1", "value");
    transactionalMap.commitTransaction();

    Assert.assertNull(transactionalMap.getAssociatedTransaction());
    Assert.assertTrue(transactionalMap.containsKey("keyWithinTransaction0"));
    Assert.assertEquals("value0", transactionalMap.get("keyWithinBothTransaction"));
    Assert.assertEquals(4, transactionalMap.size());

    transactionalMap.resumeTransaction(1);
    Assert.assertEquals("value1", transactionalMap.get("keyWithinBothTransaction"));

    transactionalMap.remove("keyAfterResumeTransaction0ButDeletedInTransaction1");

    transactionalMap.commitTransaction();

    Assert.assertFalse(
        transactionalMap.containsKey("keyAfterResumeTransaction0ButDeletedInTransaction1"));

    Assert.assertEquals("value1", transactionalMap.get("keyWithinBothTransaction"));

  }
}
