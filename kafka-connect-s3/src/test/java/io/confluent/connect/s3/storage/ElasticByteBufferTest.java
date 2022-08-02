package io.confluent.connect.s3.storage;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;
import java.nio.BufferOverflowException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ElasticByteBufferTest {

  public static final int INIT_CAP = 128 * 1024;

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalCapacity1() {
    ElasticByteBuffer buf = new ElasticByteBuffer(-1, INIT_CAP);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalCapacity2() {
    ElasticByteBuffer buf = new ElasticByteBuffer(0, INIT_CAP);
  }

  @Test
  public void testLessThanInitCapacityPut1() {
    ElasticByteBuffer buf = new ElasticByteBuffer(1024, INIT_CAP);

    assertEquals(1024, buf.physicalRemaining());
    assertEquals(1024, buf.remaining());
    assertEquals(0, buf.position());
    assertTrue(buf.hasRemaining());
    assertEquals(1024, buf.array().length);

    buf.put((byte) 0x01);
    assertEquals(1023, buf.physicalRemaining());
    assertEquals(1023, buf.remaining());
    assertEquals(1, buf.position());
    assertTrue(buf.hasRemaining());
    assertEquals(1024, buf.array().length);

    byte[] randomBytes = RandomStringUtils.randomAlphanumeric(1023).getBytes();
    for (byte randomByte : randomBytes) {
      buf.put(randomByte);
    }

    assertEquals(0, buf.physicalRemaining());
    assertEquals(0, buf.remaining());
    assertEquals(1024, buf.position());
    assertFalse(buf.hasRemaining());
    assertEquals(1024, buf.array().length);

    assertThrows(BufferOverflowException.class, () -> {
      buf.put((byte) 0x01);
    });

  }

  @Test
  public void testLessThanInitCapacityPut2() {

    int cap = 1024;
    ElasticByteBuffer buf = new ElasticByteBuffer(cap, INIT_CAP);

    byte[] randomBytes1 = RandomStringUtils.randomAlphanumeric(4).getBytes();
    buf.put(randomBytes1, 0, randomBytes1.length);

    assertEquals(1020, buf.physicalRemaining());
    assertEquals(1020, buf.remaining());
    assertEquals(4, buf.position());
    assertTrue(buf.hasRemaining());
    assertEquals(cap, buf.array().length);

    byte[] randomBytes2 = RandomStringUtils.randomAlphanumeric(1019).getBytes();
    buf.put(randomBytes2, 0, randomBytes2.length);

    assertEquals(1, buf.physicalRemaining());
    assertEquals(1, buf.remaining());
    assertEquals(1023, buf.position());
    assertTrue(buf.hasRemaining());
    assertEquals(cap, buf.array().length);

    byte[] randomBytes3 = RandomStringUtils.randomAlphanumeric(2).getBytes();
    assertThrows(BufferOverflowException.class, () -> {
      buf.put(randomBytes3, 0, randomBytes3.length);
    });

    buf.put(new byte[] {0x01}, 0, 1);
    assertEquals(0, buf.physicalRemaining());
    assertEquals(0, buf.remaining());
    assertEquals(cap, buf.position());
    assertFalse(buf.hasRemaining());
    assertEquals(cap, buf.array().length);
  }

  @Test
  public void testLessThanInitCapacityClear() {
    ElasticByteBuffer buf = new ElasticByteBuffer(1024, INIT_CAP);

    byte[] randomBytes1 = RandomStringUtils.randomAlphanumeric(4).getBytes();
    buf.put(randomBytes1, 0, randomBytes1.length);

    byte[] arrayBeforeClear = buf.array();
    buf.clear();
    byte[] arrayAfterClear = buf.array();
    assertEquals(arrayAfterClear.length, arrayBeforeClear.length);
    assertSame(arrayAfterClear, arrayBeforeClear);
  }


  @Test
  public void testGreaterThanInitCapacityPut1() {

    int cap = 10 * 1024 * 1024;
    ElasticByteBuffer buf = new ElasticByteBuffer(cap, INIT_CAP);

    assertEquals(INIT_CAP, buf.physicalRemaining());
    assertEquals(cap, buf.remaining());
    assertEquals(0, buf.position());
    assertTrue(buf.hasRemaining());
    assertEquals(INIT_CAP, buf.array().length);

    byte[] randomBytes1 = RandomStringUtils.randomAlphanumeric(INIT_CAP).getBytes();
    for (byte randomByte : randomBytes1) {
      buf.put(randomByte);
    }

    assertEquals(0, buf.physicalRemaining());
    assertEquals(cap - INIT_CAP, buf.remaining());
    assertEquals(INIT_CAP, buf.position());
    assertTrue(buf.hasRemaining());
    assertEquals(INIT_CAP, buf.array().length);

    int testBytesLen1 = 5;
    byte[] randomBytes2 = RandomStringUtils.randomAlphanumeric(testBytesLen1).getBytes();
    for (byte randomByte : randomBytes2) {
      buf.put(randomByte);
    }

    int exceptNewPhysicalSize = INIT_CAP * 2;

    assertEquals(exceptNewPhysicalSize - (INIT_CAP + testBytesLen1), buf.physicalRemaining());
    assertEquals(cap - (INIT_CAP + testBytesLen1), buf.remaining());
    assertEquals(INIT_CAP + testBytesLen1, buf.position());
    assertTrue(buf.hasRemaining());
    assertEquals(exceptNewPhysicalSize, buf.array().length);

    int remaining = cap - (INIT_CAP + testBytesLen1);
    byte[] randomBytes3 = RandomStringUtils.randomAlphanumeric(remaining).getBytes();
    for (byte randomByte : randomBytes3) {
      buf.put(randomByte);
    }

    assertEquals(0, buf.physicalRemaining());
    assertEquals(0, buf.remaining());
    assertEquals(cap, buf.position());
    assertFalse(buf.hasRemaining());
    assertEquals(cap, buf.array().length);

    assertThrows(BufferOverflowException.class, ()->{
      buf.put((byte) 0x01);
    });
  }

  @Test
  public void testGreaterThanInitCapacityPut2() {
    int cap = 10 * 1024 * 1024;
    ElasticByteBuffer buf = new ElasticByteBuffer(cap, INIT_CAP);

    assertEquals(INIT_CAP, buf.physicalRemaining());
    assertEquals(cap, buf.remaining());
    assertEquals(0, buf.position());
    assertTrue(buf.hasRemaining());
    assertEquals(INIT_CAP, buf.array().length);

    byte[] randomBytes1 = RandomStringUtils.randomAlphanumeric(INIT_CAP).getBytes();
    buf.put(randomBytes1, 0, randomBytes1.length);

    assertEquals(0, buf.physicalRemaining());
    assertEquals(cap - INIT_CAP, buf.remaining());
    assertEquals(INIT_CAP, buf.position());
    assertTrue(buf.hasRemaining());
    assertEquals(INIT_CAP, buf.array().length);

    int testBytesLen1 = 5;
    byte[] randomBytes2 = RandomStringUtils.randomAlphanumeric(testBytesLen1).getBytes();
    buf.put(randomBytes2, 0, randomBytes2.length);

    int expectNewPhysicalSize = INIT_CAP * 2;

    assertEquals(expectNewPhysicalSize - (INIT_CAP + testBytesLen1), buf.physicalRemaining());
    assertEquals(cap - (INIT_CAP + testBytesLen1), buf.remaining());
    assertEquals(INIT_CAP + testBytesLen1, buf.position());
    assertTrue(buf.hasRemaining());
    assertEquals(expectNewPhysicalSize, buf.array().length);

    int remainingLessOne = cap - (INIT_CAP + testBytesLen1) - 1;
    byte[] randomBytes3 = RandomStringUtils.randomAlphanumeric(remainingLessOne).getBytes();
    buf.put(randomBytes3, 0, randomBytes3.length);

    assertEquals(1, buf.physicalRemaining());
    assertEquals(1, buf.remaining());
    assertEquals(cap - 1, buf.position());
    assertTrue(buf.hasRemaining());
    assertEquals(cap, buf.array().length);

    assertThrows(BufferOverflowException.class, ()->{
      buf.put(new byte[] {0x01, 0x02}, 0, 2);
    });

    buf.put(new byte[] {0x01}, 0, 1);
    assertEquals(0, buf.physicalRemaining());
    assertEquals(0, buf.remaining());
    assertEquals(cap, buf.position());
    assertFalse(buf.hasRemaining());
    assertEquals(cap, buf.array().length);

  }

  @Test
  public void testGreaterThanInitCapacityClear() {
    int cap = 10 * 1024 * 1024;
    ElasticByteBuffer buf = new ElasticByteBuffer(cap, INIT_CAP);

    byte[] randomBytes1 = RandomStringUtils.randomAlphanumeric(5 * 1024 * 1024).getBytes();
    buf.put(randomBytes1, 0, randomBytes1.length);

    byte[] arrayBeforeClear = buf.array();
    buf.clear();
    byte[] arrayAfterClear = buf.array();

    assertEquals(0, buf.position());
    assertTrue(buf.hasRemaining());
    assertEquals(INIT_CAP, buf.physicalRemaining());
    assertEquals(cap, buf.remaining());

    assertEquals(INIT_CAP, arrayAfterClear.length);
    assertTrue(arrayAfterClear.length < arrayBeforeClear.length);
    assertNotSame(arrayAfterClear, arrayBeforeClear);
  }

  @Test
  public void testLessThanInitSizeDataPut1() {
    int cap = 1024;
    ElasticByteBuffer buf = new ElasticByteBuffer(cap, INIT_CAP);

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
    ElasticByteBuffer buf = new ElasticByteBuffer(cap, INIT_CAP);

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
    ElasticByteBuffer buf = new ElasticByteBuffer(cap, INIT_CAP);

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
    ElasticByteBuffer buf = new ElasticByteBuffer(cap, INIT_CAP);

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