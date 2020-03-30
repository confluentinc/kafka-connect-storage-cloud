package io.confluent.connect.s3.storage;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.BufferOverflowException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticByteBufferTest {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalCapacity1() {
    ElasticByteBuffer buf = new ElasticByteBuffer(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalCapacity2() {
    ElasticByteBuffer buf = new ElasticByteBuffer(0);
  }

  @Test
  public void testLessThanInitCapacityPut1() {
    ElasticByteBuffer buf = new ElasticByteBuffer(1024);

    assertEquals(1024, buf.physicalRemaining());
    assertEquals(1024, buf.remaining());
    assertEquals(0, buf.position());
    assertEquals(true, buf.hasRemaining());
    assertEquals(1024, buf.array().length);

    buf.put((byte) 0x01);
    assertEquals(1023, buf.physicalRemaining());
    assertEquals(1023, buf.remaining());
    assertEquals(1, buf.position());
    assertEquals(true, buf.hasRemaining());
    assertEquals(1024, buf.array().length);

    byte[] randomBytes = RandomStringUtils.randomAlphanumeric(1023).getBytes();
    for (byte randomByte : randomBytes) {
      buf.put(randomByte);
    }

    assertEquals(0, buf.physicalRemaining());
    assertEquals(0, buf.remaining());
    assertEquals(1024, buf.position());
    assertEquals(false, buf.hasRemaining());
    assertEquals(1024, buf.array().length);

    exceptionRule.expect(BufferOverflowException.class);
    buf.put((byte) 0x01);
  }

  @Test
  public void testLessThanInitCapacityPut2() {
    ElasticByteBuffer buf = new ElasticByteBuffer(1024);

    byte[] randomBytes1 = RandomStringUtils.randomAlphanumeric(4).getBytes();
    buf.put(randomBytes1, 0, randomBytes1.length);

    assertEquals(1020, buf.physicalRemaining());
    assertEquals(1020, buf.remaining());
    assertEquals(4, buf.position());
    assertEquals(true, buf.hasRemaining());
    assertEquals(1024, buf.array().length);

    byte[] randomBytes2 = RandomStringUtils.randomAlphanumeric(1020).getBytes();
    buf.put(randomBytes2, 0, randomBytes2.length);

    assertEquals(0, buf.physicalRemaining());
    assertEquals(0, buf.remaining());
    assertEquals(1024, buf.position());
    assertEquals(false, buf.hasRemaining());
    assertEquals(1024, buf.array().length);

    byte[] randomBytes3 = RandomStringUtils.randomAlphanumeric(2).getBytes();
    exceptionRule.expect(BufferOverflowException.class);
    buf.put(randomBytes3, 0, randomBytes3.length);
  }

  @Test
  public void testLessThanInitCapacityClear() {
    ElasticByteBuffer buf = new ElasticByteBuffer(1024);

    byte[] randomBytes1 = RandomStringUtils.randomAlphanumeric(4).getBytes();
    buf.put(randomBytes1, 0, randomBytes1.length);

    byte[] arrayBeforeClear = buf.array();
    buf.clear();
    byte[] arrayAfterClear = buf.array();
    assertTrue(arrayAfterClear.length == arrayBeforeClear.length);
    assertTrue(arrayAfterClear == arrayBeforeClear);
  }


  @Test
  public void testGreaterThanInitCapacityPut1() {

    int cap = 10 * 1024 * 1024;
    ElasticByteBuffer buf = new ElasticByteBuffer(cap);

    assertEquals(ElasticByteBuffer.INIT_CAPACITY, buf.physicalRemaining());
    assertEquals(cap, buf.remaining());
    assertEquals(0, buf.position());
    assertEquals(true, buf.hasRemaining());
    assertEquals(ElasticByteBuffer.INIT_CAPACITY, buf.array().length);

    byte[] randomBytes1 = RandomStringUtils.randomAlphanumeric(ElasticByteBuffer.INIT_CAPACITY).getBytes();
    for (byte randomByte : randomBytes1) {
      buf.put(randomByte);
    }

    assertEquals(0, buf.physicalRemaining());
    assertEquals(cap - ElasticByteBuffer.INIT_CAPACITY, buf.remaining());
    assertEquals(ElasticByteBuffer.INIT_CAPACITY, buf.position());
    assertEquals(true, buf.hasRemaining());
    assertEquals(ElasticByteBuffer.INIT_CAPACITY, buf.array().length);

    int testBytesLen1 = 5;
    byte[] randomBytes2 = RandomStringUtils.randomAlphanumeric(testBytesLen1).getBytes();
    for (byte randomByte : randomBytes2) {
      buf.put(randomByte);
    }

    int exceptNewPhysicalSize = ElasticByteBuffer.INIT_CAPACITY * 2;

    assertEquals(exceptNewPhysicalSize - (ElasticByteBuffer.INIT_CAPACITY + testBytesLen1), buf.physicalRemaining());
    assertEquals(cap - (ElasticByteBuffer.INIT_CAPACITY + testBytesLen1), buf.remaining());
    assertEquals(ElasticByteBuffer.INIT_CAPACITY + testBytesLen1, buf.position());
    assertEquals(true, buf.hasRemaining());
    assertEquals(exceptNewPhysicalSize, buf.array().length);

    int remaining = cap - (ElasticByteBuffer.INIT_CAPACITY + testBytesLen1);
    byte[] randomBytes3 = RandomStringUtils.randomAlphanumeric(remaining).getBytes();
    for (byte randomByte : randomBytes3) {
      buf.put(randomByte);
    }

    assertEquals(0, buf.physicalRemaining());
    assertEquals(0, buf.remaining());
    assertEquals(cap, buf.position());
    assertEquals(false, buf.hasRemaining());
    assertEquals(cap, buf.array().length);

    exceptionRule.expect(BufferOverflowException.class);
    buf.put((byte) 0x01);
  }

  @Test
  public void testGreaterThanInitCapacityPut2() {
    int cap = 10 * 1024 * 1024;
    ElasticByteBuffer buf = new ElasticByteBuffer(cap);

    assertEquals(ElasticByteBuffer.INIT_CAPACITY, buf.physicalRemaining());
    assertEquals(cap, buf.remaining());
    assertEquals(0, buf.position());
    assertEquals(true, buf.hasRemaining());
    assertEquals(ElasticByteBuffer.INIT_CAPACITY, buf.array().length);

    byte[] randomBytes1 = RandomStringUtils.randomAlphanumeric(ElasticByteBuffer.INIT_CAPACITY).getBytes();
    buf.put(randomBytes1, 0, randomBytes1.length);

    assertEquals(0, buf.physicalRemaining());
    assertEquals(cap - ElasticByteBuffer.INIT_CAPACITY, buf.remaining());
    assertEquals(ElasticByteBuffer.INIT_CAPACITY, buf.position());
    assertEquals(true, buf.hasRemaining());
    assertEquals(ElasticByteBuffer.INIT_CAPACITY, buf.array().length);

    int testBytesLen1 = 5;
    byte[] randomBytes2 = RandomStringUtils.randomAlphanumeric(testBytesLen1).getBytes();
    buf.put(randomBytes2, 0, randomBytes2.length);

    int exceptNewPhysicalSize = ElasticByteBuffer.INIT_CAPACITY * 2;

    assertEquals(exceptNewPhysicalSize - (ElasticByteBuffer.INIT_CAPACITY + testBytesLen1), buf.physicalRemaining());
    assertEquals(cap - (ElasticByteBuffer.INIT_CAPACITY + testBytesLen1), buf.remaining());
    assertEquals(ElasticByteBuffer.INIT_CAPACITY + testBytesLen1, buf.position());
    assertEquals(true, buf.hasRemaining());
    assertEquals(exceptNewPhysicalSize, buf.array().length);

    int remaining = cap - (ElasticByteBuffer.INIT_CAPACITY + testBytesLen1);
    byte[] randomBytes3 = RandomStringUtils.randomAlphanumeric(remaining).getBytes();
    buf.put(randomBytes3, 0, randomBytes3.length);

    assertEquals(0, buf.physicalRemaining());
    assertEquals(0, buf.remaining());
    assertEquals(cap, buf.position());
    assertEquals(false, buf.hasRemaining());
    assertEquals(cap, buf.array().length);

    exceptionRule.expect(BufferOverflowException.class);
    buf.put(new byte[] {0x01}, 0, 1);
  }

  @Test
  public void testGreaterThanInitCapacityClear() {
    int cap = 10 * 1024 * 1024;
    ElasticByteBuffer buf = new ElasticByteBuffer(cap);

    byte[] randomBytes1 = RandomStringUtils.randomAlphanumeric(5 * 1024 * 1024).getBytes();
    buf.put(randomBytes1, 0, randomBytes1.length);

    byte[] arrayBeforeClear = buf.array();
    buf.clear();
    byte[] arrayAfterClear = buf.array();

    assertEquals(0, buf.position());
    assertEquals(true, buf.hasRemaining());
    assertEquals(ElasticByteBuffer.INIT_CAPACITY, buf.physicalRemaining());
    assertEquals(cap, buf.remaining());

    assertEquals(ElasticByteBuffer.INIT_CAPACITY, arrayAfterClear.length);
    assertTrue(arrayAfterClear.length < arrayBeforeClear.length);
    assertTrue(arrayAfterClear != arrayBeforeClear);
  }

  @Test
  public void testLessThanInitSizeDataPut1() {
    int cap = 1024;
    ElasticByteBuffer buf = new ElasticByteBuffer(cap);

    int testBytesLen1 = 4;
    String data1 = RandomStringUtils.randomAlphanumeric(testBytesLen1);
    byte[] randomBytes1 = data1.getBytes();
    for (byte randomByte : randomBytes1) {
      buf.put(randomByte);
    }

    assertEquals(data1, new String(buf.array(), 0, buf.position()));

    int testBytesLen2 = 1020;
    String data2 = RandomStringUtils.randomAlphanumeric(testBytesLen2);
    byte[] randomBytes2 = data2.getBytes();
    for (byte randomByte : randomBytes2) {
      buf.put(randomByte);
    }

    assertEquals(data1 + data2, new String(buf.array(), 0, buf.position()));
  }

  @Test
  public void testLessThanInitSizeDataPut2() {
    int cap = 1024;
    ElasticByteBuffer buf = new ElasticByteBuffer(cap);

    int testBytesLen1 = 4;
    String data1 = RandomStringUtils.randomAlphanumeric(testBytesLen1);
    byte[] randomBytes1 = data1.getBytes();
    buf.put(randomBytes1, 0, randomBytes1.length);

    assertEquals(data1, new String(buf.array(), 0, buf.position()));

    int testBytesLen2 = 1020;
    String data2 = RandomStringUtils.randomAlphanumeric(testBytesLen2);
    byte[] randomBytes2 = data2.getBytes();
    buf.put(randomBytes2, 0, randomBytes2.length);

    assertEquals(data1 + data2, new String(buf.array(), 0, buf.position()));
  }

  @Test
  public void testGreaterThanInitSizeDataPut1() {
    int cap = 5 * 1024 * 1024;
    ElasticByteBuffer buf = new ElasticByteBuffer(cap);

    int testBytesLen1 = RandomUtils.nextInt(cap);
    String data1 = RandomStringUtils.randomAlphanumeric(testBytesLen1);
    byte[] randomBytes1 = data1.getBytes();
    for (byte randomByte : randomBytes1) {
      buf.put(randomByte);
    }

    assertEquals(data1, new String(buf.array(), 0, buf.position()));

    int testBytesLen2 = cap - testBytesLen1;
    String data2 = RandomStringUtils.randomAlphanumeric(testBytesLen2);
    byte[] randomBytes2 = data2.getBytes();
    for (byte randomByte : randomBytes2) {
      buf.put(randomByte);
    }

    assertEquals(data1 + data2, new String(buf.array(), 0, buf.position()));
  }

  @Test
  public void testGreaterThanInitSizeDataPut2() {
    int cap = 5 * 1024 * 1024;
    ElasticByteBuffer buf = new ElasticByteBuffer(cap);

    int testBytesLen1 = RandomUtils.nextInt(cap);
    String data1 = RandomStringUtils.randomAlphanumeric(testBytesLen1);
    byte[] randomBytes1 = data1.getBytes();
    buf.put(randomBytes1, 0, randomBytes1.length);

    assertEquals(data1, new String(buf.array(), 0, buf.position()));

    int testBytesLen2 = cap - testBytesLen1;
    String data2 = RandomStringUtils.randomAlphanumeric(testBytesLen2);
    byte[] randomBytes2 = data2.getBytes();
    buf.put(randomBytes2, 0, randomBytes2.length);

    assertEquals(data1 + data2, new String(buf.array(), 0, buf.position()));
  }
}