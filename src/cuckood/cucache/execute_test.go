package main

import (
	"cuckood"
	"encoding/binary"
	"testing"
	"time"

	gomem "github.com/dustin/gomemcached"
)

// These tests are all taken from the brilliant memcached-test Python
// application here: https://github.com/dustin/memcached-test
// and have been translated into Go tests

/*
   def testVersion(self):
       """Test the version command returns something."""
       v=self.mc.version()
       self.assertTrue(len(v) > 0, "Bad version:  ``" + str(v) + "''")
*/
func TestVersion(t *testing.T) {
	c := cuckoo.New(0)
	res := do(c, t, &gomem.MCRequest{
		Opcode: gomem.VERSION,
	})
	version := string(res.Body)
	if len(version) == 0 {
		t.Error("Empty version string given")
	}
}

/*
   def testSimpleSetGet(self):
       """Test a simple set and get."""
       self.mc.set("x", 5, 19, "somevalue")
       self.assertGet((19, "somevalue"), self.mc.get("x"))
*/
func TestSimpleSetGet(t *testing.T) {
	c := cuckoo.New(0)
	assertSet(c, t, "x", []byte("somevalue"), gomem.SET)
	assertGet(c, t, "x", []byte("somevalue"))
}

/*
   def testZeroExpiration(self):
       """Ensure zero-expiration sets work properly."""
       self.mc.set("x", 0, 19, "somevalue")
       time.sleep(1.1)
       self.assertGet((19, "somevalue"), self.mc.get("x"))
*/
func TestZeroExpiration(t *testing.T) {
	c := cuckoo.New(0)
	assertSet(c, t, "x", []byte("somevalue"), gomem.SET)
	time.Sleep(1*time.Second + 100*time.Millisecond)
	assertGet(c, t, "x", []byte("somevalue"))
}

/*
   def testDelete(self):
       """Test a set, get, delete, get sequence."""
       self.mc.set("x", 5, 19, "somevalue")
       self.assertGet((19, "somevalue"), self.mc.get("x"))
       self.mc.delete("x")
       self.assertNotExists("x")
*/
func TestDelete(t *testing.T) {
	c := cuckoo.New(0)
	assertSet(c, t, "x", []byte("somevalue"), gomem.SET)
	assertGet(c, t, "x", []byte("somevalue"))
	do(c, t, &gomem.MCRequest{
		Opcode: gomem.DELETE,
		Key:    []byte("x"),
	})
	assertNotExists(c, t, "x")
}

/*
   def testFlush(self):
       """Test flushing."""
       self.mc.set("x", 5, 19, "somevaluex")
       self.mc.set("y", 5, 17, "somevaluey")
       self.assertGet((19, "somevaluex"), self.mc.get("x"))
       self.assertGet((17, "somevaluey"), self.mc.get("y"))
       self.mc.flush()
       self.assertNotExists("x")
       self.assertNotExists("y")
*/
func TestFlush(t *testing.T) {
	c := cuckoo.New(0)
	assertSet(c, t, "x", []byte("somevaluex"), gomem.SET)
	assertSet(c, t, "y", []byte("somevaluey"), gomem.SET)
	assertGet(c, t, "x", []byte("somevaluex"))
	assertGet(c, t, "y", []byte("somevaluey"))
	do(c, t, &gomem.MCRequest{
		Opcode: gomem.FLUSH,
		Extras: noexp,
	})
	assertNotExists(c, t, "x")
	assertNotExists(c, t, "y")
}

/*
   def testNoop(self):
       """Making sure noop is understood."""
       self.mc.noop()
*/
func TestNoop(t *testing.T) {
	c := cuckoo.New(0)
	do(c, t, &gomem.MCRequest{
		Opcode: gomem.NOOP,
	})
}

/*
   def testAdd(self):
       """Test add functionality."""
       self.assertNotExists("x")
       self.mc.add("x", 5, 19, "ex")
       self.assertGet((19, "ex"), self.mc.get("x"))
       try:
           self.mc.add("x", 5, 19, "ex2")
           self.fail("Expected failure to add existing key")
       except MemcachedError, e:
           self.assertEquals(memcacheConstants.ERR_EXISTS, e.status)
       self.assertGet((19, "ex"), self.mc.get("x"))
*/
func TestAdd(t *testing.T) {
	c := cuckoo.New(0)
	assertNotExists(c, t, "x")

	assertSet(c, t, "x", []byte("ex"), gomem.ADD)
	assertGet(c, t, "x", []byte("ex"))

	res := set(c, "x", []byte("ex2"), gomem.ADD)
	if res.Status != gomem.KEY_EEXISTS {
		t.Errorf("expected add on existing key to fail, got %v", res.Status)
	}

	assertGet(c, t, "x", []byte("ex"))
}

/*
   def testReplace(self):
       """Test replace functionality."""
       self.assertNotExists("x")
       try:
           self.mc.replace("x", 5, 19, "ex")
           self.fail("Expected failure to replace missing key")
       except MemcachedError, e:
           self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
       self.mc.add("x", 5, 19, "ex")
       self.assertGet((19, "ex"), self.mc.get("x"))
       self.mc.replace("x", 5, 19, "ex2")
       self.assertGet((19, "ex2"), self.mc.get("x"))
*/
func TestReplace(t *testing.T) {
	c := cuckoo.New(0)
	assertNotExists(c, t, "x")

	res := set(c, "x", []byte("ex"), gomem.REPLACE)
	if res.Status != gomem.KEY_ENOENT {
		t.Errorf("expected replace on non-existing key to fail, got %v", res.Status)
	}

	assertSet(c, t, "x", []byte("ex"), gomem.ADD)
	assertGet(c, t, "x", []byte("ex"))
	assertSet(c, t, "x", []byte("ex2"), gomem.REPLACE)
	assertGet(c, t, "x", []byte("ex2"))
}

/*
   def testMultiGet(self):
       """Testing multiget functionality"""
       self.mc.add("x", 5, 1, "ex")
       self.mc.add("y", 5, 2, "why")
       vals=self.mc.getMulti('xyz')
       self.assertGet((1, 'ex'), vals['x'])
       self.assertGet((2, 'why'), vals['y'])
       self.assertEquals(2, len(vals))

       XXX: multi-get is the same as single get as far as req2res is concerned
*/

/*
   def testIncrDoesntExistNoCreate(self):
       """Testing incr when a value doesn't exist (and not creating)."""
       try:
           self.mc.incr("x", exp=memcacheConstants.INCRDECR_SPECIAL)
           self.fail("Expected failure to increment non-existent key")
       except MemcachedError, e:
           self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
       self.assertNotExists("x")
*/
func TestIncrDoesntExistNoCreate(t *testing.T) {
	c := cuckoo.New(0)
	res := pm(c, "x", 1, 1, true, gomem.INCREMENT)
	if res.Status != gomem.KEY_ENOENT {
		t.Errorf("expected incr on non-existing key to fail, got %v", res.Status)
	}
}

/*
   def testIncrDoesntExistCreate(self):
       """Testing incr when a value doesn't exist (and we make a new one)"""
       self.assertNotExists("x")
       self.assertEquals(19, self.mc.incr("x", init=19)[0])
*/
func TestIncrDoesntExistCreate(t *testing.T) {
	c := cuckoo.New(0)
	assertPM(c, t, "x", 0, 19, false, gomem.INCREMENT)
	assertGet(c, t, "x", []byte("19"))
}

/*
   def testDecrDoesntExistNoCreate(self):
       """Testing decr when a value doesn't exist (and not creating)."""
       try:
           self.mc.decr("x", exp=memcacheConstants.INCRDECR_SPECIAL)
           self.fail("Expected failiure to decrement non-existent key.")
       except MemcachedError, e:
           self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
       self.assertNotExists("x")
*/
func TestDecrDoesntExistNoCreate(t *testing.T) {
	c := cuckoo.New(0)
	res := pm(c, "x", 1, 1, true, gomem.DECREMENT)
	if res.Status != gomem.KEY_ENOENT {
		t.Errorf("expected decr on non-existing key to fail, got %v", res.Status)
	}
}

/*
   def testDecrDoesntExistCreate(self):
       """Testing decr when a value doesn't exist (and we make a new one)"""
       self.assertNotExists("x")
       self.assertEquals(19, self.mc.decr("x", init=19)[0])
*/
func TestDecrDoesntExistCreate(t *testing.T) {
	c := cuckoo.New(0)
	assertPM(c, t, "x", 0, 19, false, gomem.DECREMENT)
	assertGet(c, t, "x", []byte("19"))
}

/*

   def testIncr(self):
       """Simple incr test."""
       val, cas=self.mc.incr("x")
       self.assertEquals(0, val)
       val, cas=self.mc.incr("x")
       self.assertEquals(1, val)
       val, cas=self.mc.incr("x", 211)
       self.assertEquals(212, val)
       val, cas=self.mc.incr("x", 2**33)
       self.assertEquals(8589934804L, val)
*/
func TestIncr(t *testing.T) {
	c := cuckoo.New(0)
	assertPM(c, t, "x", 1, 0, false, gomem.INCREMENT)
	assertGet(c, t, "x", []byte("0"))
	assertPM(c, t, "x", 1, 0, false, gomem.INCREMENT)
	assertGet(c, t, "x", []byte("1"))
	assertPM(c, t, "x", 211, 0, false, gomem.INCREMENT)
	assertGet(c, t, "x", []byte("212"))
	assertPM(c, t, "x", 1<<33, 0, false, gomem.INCREMENT)
	assertGet(c, t, "x", []byte("8589934804"))
}

/*

   def testDecr(self):
       """Simple decr test."""
       val, cas=self.mc.incr("x", init=5)
       self.assertEquals(5, val)
       val, cas=self.mc.decr("x")
       self.assertEquals(4, val)
       val, cas=self.mc.decr("x", 211)
       self.assertEquals(0, val)
*/
func TestDecr(t *testing.T) {
	c := cuckoo.New(0)
	assertPM(c, t, "x", 1, 5, false, gomem.DECREMENT)
	assertGet(c, t, "x", []byte("5"))
	assertPM(c, t, "x", 1, 0, false, gomem.DECREMENT)
	assertGet(c, t, "x", []byte("4"))
	assertPM(c, t, "x", 211, 0, false, gomem.DECREMENT)
	assertGet(c, t, "x", []byte("0"))
}

/*

   def testCas(self):
       """Test CAS operation."""
       try:
           self.mc.cas("x", 5, 19, 0x7fffffffff, "bad value")
           self.fail("Expected error CASing with no existing value")
       except MemcachedError, e:
           self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
       self.mc.add("x", 5, 19, "original value")
       flags, i, val=self.mc.get("x")
       self.assertEquals("original value", val)
       try:
           self.mc.cas("x", 5, 19, i+1, "broken value")
           self.fail("Expected error CASing with invalid id")
       except MemcachedError, e:
           self.assertEquals(memcacheConstants.ERR_EXISTS, e.status)
       self.mc.cas("x", 5, 19, i, "new value")
       newflags, newi, newval=self.mc.get("x")
       self.assertEquals("new value", newval)

       # Test a CAS replay
       try:
           self.mc.cas("x", 5, 19, i, "crap value")
           self.fail("Expected error CASing with invalid id")
       except MemcachedError, e:
           self.assertEquals(memcacheConstants.ERR_EXISTS, e.status)
       newflags, newi, newval=self.mc.get("x")
       self.assertEquals("new value", newval)
*/
func TestCas(t *testing.T) {
	c := cuckoo.New(0)
	req := &gomem.MCRequest{
		Opcode: gomem.SET,
		Key:    []byte("x"),
		Body:   []byte("bad value"),
		Extras: nullset,
		Cas:    0x7fffffffff,
	}

	res := req2res(c, req)
	if res.Status != gomem.KEY_ENOENT {
		t.Errorf("expected cas on non-existent key to fail with ERR_NOT_FOUND, got %v", res.Status)
	}

	assertSet(c, t, "x", []byte("original value"), gomem.ADD)
	res = assertGet(c, t, "x", []byte("original value"))

	req.Cas = res.Cas + 1
	req.Body = []byte("broken value")
	res = req2res(c, req)
	if res.Status != gomem.KEY_EEXISTS {
		t.Errorf("expected set with invalid cas to fail with EEXISTS, got %v", res.Status)
	}

	req.Cas = req.Cas - 1
	req.Body = []byte("new value")
	res = req2res(c, req)
	if res.Status != gomem.SUCCESS {
		t.Errorf("expected set with valid cas to succeed, got %v", res.Status)
	}

	res = assertGet(c, t, "x", []byte("new value"))
	req.Body = []byte("crap value")
	res = req2res(c, req)
	if res.Status != gomem.KEY_EEXISTS {
		t.Errorf("expected replayed cas to fail with EEXISTS, got %v", res.Status)
	}

	assertGet(c, t, "x", []byte("new value"))
}

/*
   # Assert we know the correct CAS for a given key.
   def assertValidCas(self, key, cas):
       flags, currentcas, val=self.mc.get(key)
       self.assertEquals(currentcas, cas)

   def testSetReturnsCas(self):
       """Ensure a set command returns the current CAS."""
       vals=self.mc.set('x', 5, 19, 'some val')
       self.assertValidCas('x', vals[1])
*/
func TestSetReturnsCas(t *testing.T) {
	c := cuckoo.New(0)
	res_set := assertSet(c, t, "x", []byte("some val"), gomem.SET)
	res_get := assertGet(c, t, "x", []byte("some val"))
	if res_set.Cas != res_get.Cas {
		t.Errorf("expected CAS from SET to match CAS from GET (s: %d, g: %d)", res_set.Cas, res_get.Cas)
	}
}

/*
   def testAddReturnsCas(self):
       """Ensure an add command returns the current CAS."""
       vals=self.mc.add('x', 5, 19, 'some val')
       self.assertValidCas('x', vals[1])
*/
func TestAddReturnsCas(t *testing.T) {
	c := cuckoo.New(0)
	res_set := assertSet(c, t, "x", []byte("some val"), gomem.ADD)
	res_get := assertGet(c, t, "x", []byte("some val"))
	if res_set.Cas != res_get.Cas {
		t.Errorf("expected CAS from ADD to match CAS from GET (a: %d, g: %d)", res_set.Cas, res_get.Cas)
	}
}

/*
   def testReplaceReturnsCas(self):
       """Ensure a replace command returns the current CAS."""
       vals=self.mc.add('x', 5, 19, 'some val')
       vals=self.mc.replace('x', 5, 19, 'other val')
       self.assertValidCas('x', vals[1])
*/
func TestReplaceReturnsCas(t *testing.T) {
	c := cuckoo.New(0)
	assertSet(c, t, "x", []byte("some val"), gomem.ADD)
	res_set := assertSet(c, t, "x", []byte("other val"), gomem.REPLACE)
	res_get := assertGet(c, t, "x", []byte("other val"))
	if res_set.Cas != res_get.Cas {
		t.Errorf("expected CAS from REPLACE to match CAS from GET (r: %d, g: %d)", res_set.Cas, res_get.Cas)
	}
}

/*
   def testIncrReturnsCAS(self):
       """Ensure an incr command returns the current CAS."""
       val, cas, something=self.mc.set("x", 5, 19, '4')
       val, cas=self.mc.incr("x", init=5)
       self.assertEquals(5, val)
       self.assertValidCas('x', cas)
*/
func TestIncrReturnsCas(t *testing.T) {
	c := cuckoo.New(0)
	assertSet(c, t, "x", []byte("4"), gomem.ADD)
	res_set := assertPM(c, t, "x", 1, 5, true, gomem.INCREMENT)
	res_get := assertGet(c, t, "x", []byte("5"))
	if res_set.Cas != res_get.Cas {
		t.Errorf("expected CAS from INCR to match CAS from GET (i: %d, g: %d)", res_set.Cas, res_get.Cas)
	}
}

/*

   def testDecrReturnsCAS(self):
       """Ensure an decr command returns the current CAS."""
       val, cas, something=self.mc.set("x", 5, 19, '4')
       val, cas=self.mc.decr("x", init=5)
       self.assertEquals(3, val)
       self.assertValidCas('x', cas)
*/
func TestDecrReturnsCas(t *testing.T) {
	c := cuckoo.New(0)
	assertSet(c, t, "x", []byte("4"), gomem.ADD)
	res_set := assertPM(c, t, "x", 1, 5, true, gomem.DECREMENT)
	res_get := assertGet(c, t, "x", []byte("3"))
	if res_set.Cas != res_get.Cas {
		t.Errorf("expected CAS from DECR to match CAS from GET (d: %d, g: %d)", res_set.Cas, res_get.Cas)
	}
}

/*
   def testDeletionCAS(self):
       """Validation deletion honors cas."""
       try:
           self.mc.delete("x")
       except MemcachedError, e:
           self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
       val, cas, something=self.mc.set("x", 5, 19, '4')
       try:
           self.mc.delete('x', cas=cas+1)
           self.fail("Deletion should've failed.")
       except MemcachedError, e:
           self.assertEquals(memcacheConstants.ERR_EXISTS, e.status)
       self.assertGet((19, '4'), self.mc.get('x'))
       self.mc.delete('x', cas=cas)
       self.assertNotExists('x')
*/
func TestDeletionCAS(t *testing.T) {
	c := cuckoo.New(0)
	req := &gomem.MCRequest{
		Opcode: gomem.DELETE,
		Key:    []byte("x"),
	}

	res := req2res(c, req)
	if res.Status != gomem.KEY_ENOENT {
		t.Errorf("expected delete of non-existing key to fail with ENOENT, got %v", res.Status)
	}

	res = assertSet(c, t, "x", []byte("4"), gomem.ADD)
	req.Cas = res.Cas + 1
	res = req2res(c, req)
	if res.Status != gomem.KEY_EEXISTS {
		t.Errorf("expected delete with invalid cas to fail with EEXISTS, got %v", res.Status)
	}

	assertGet(c, t, "x", []byte("4"))
	req.Cas = req.Cas - 1
	res = req2res(c, req)
	if res.Status != gomem.SUCCESS {
		t.Errorf("expected delete with valid cas to succeed, got %v", res.Status)
	}

	assertNotExists(c, t, "x")
}

/*
   def testAppend(self):
       """Test append functionality."""
       val, cas, something=self.mc.set("x", 5, 19, "some")
       val, cas, something=self.mc.append("x", "thing")
       self.assertGet((19, 'something'), self.mc.get("x"))
*/
func TestAppend(t *testing.T) {
	c := cuckoo.New(0)

	assertSet(c, t, "x", []byte("some"), gomem.SET)
	do(c, t, &gomem.MCRequest{
		Opcode: gomem.APPEND,
		Key:    []byte("x"),
		Body:   []byte("thing"),
	})
	assertGet(c, t, "x", []byte("something"))
}

/*
   def testAppendCAS(self):
       """Test append functionality honors CAS."""
       val, cas, something=self.mc.set("x", 5, 19, "some")
       try:
           val, cas, something=self.mc.append("x", "thing", cas+1)
           self.fail("expected CAS failure.")
       except MemcachedError, e:
           self.assertEquals(memcacheConstants.ERR_EXISTS, e.status)
       self.assertGet((19, 'some'), self.mc.get("x"))
*/
func TestAppendCAS(t *testing.T) {
	c := cuckoo.New(0)
	req := &gomem.MCRequest{
		Opcode: gomem.APPEND,
		Key:    []byte("x"),
		Body:   []byte("thing"),
	}

	res := assertSet(c, t, "x", []byte("some"), gomem.ADD)
	req.Cas = res.Cas + 1
	res = req2res(c, req)
	if res.Status != gomem.KEY_EEXISTS {
		t.Errorf("expected append with invalid cas to fail with EEXISTS, got %v", res.Status)
	}
	assertGet(c, t, "x", []byte("some"))
}

/*
   def testPrepend(self):
       """Test prepend functionality."""
       val, cas, something=self.mc.set("x", 5, 19, "some")
       val, cas, something=self.mc.prepend("x", "thing")
       self.assertGet((19, 'thingsome'), self.mc.get("x"))
*/
func TestPrepend(t *testing.T) {
	c := cuckoo.New(0)

	assertSet(c, t, "x", []byte("some"), gomem.SET)
	do(c, t, &gomem.MCRequest{
		Opcode: gomem.PREPEND,
		Key:    []byte("x"),
		Body:   []byte("thing"),
	})
	assertGet(c, t, "x", []byte("thingsome"))
}

/*
   def testPrependCAS(self):
       """Test prepend functionality honors CAS."""
       val, cas, something=self.mc.set("x", 5, 19, "some")
       try:
           val, cas, something=self.mc.prepend("x", "thing", cas+1)
           self.fail("expected CAS failure.")
       except MemcachedError, e:
           self.assertEquals(memcacheConstants.ERR_EXISTS, e.status)
       self.assertGet((19, 'some'), self.mc.get("x"))
*/
func TestPrependCAS(t *testing.T) {
	c := cuckoo.New(0)
	req := &gomem.MCRequest{
		Opcode: gomem.PREPEND,
		Key:    []byte("x"),
		Body:   []byte("thing"),
	}

	res := assertSet(c, t, "x", []byte("some"), gomem.ADD)
	req.Cas = res.Cas + 1
	res = req2res(c, req)
	if res.Status != gomem.KEY_EEXISTS {
		t.Errorf("expected prepend with invalid cas to fail with EEXISTS, got %v", res.Status)
	}
	assertGet(c, t, "x", []byte("some"))
}

/*
   def testTimeBombedFlush(self):
       """Test a flush with a time bomb."""
       val, cas, something=self.mc.set("x", 5, 19, "some")
       self.mc.flush(2)
       self.assertGet((19, 'some'), self.mc.get("x"))
       time.sleep(2.1)
       self.assertNotExists('x')
*/
func TestTimeBombedFlush(t *testing.T) {
	c := cuckoo.New(0)
	assertSet(c, t, "x", []byte("some"), gomem.ADD)

	exp := make([]byte, 4)
	binary.BigEndian.PutUint32(exp, 2)
	do(c, t, &gomem.MCRequest{
		Opcode: gomem.FLUSH,
		Extras: exp,
	})

	assertGet(c, t, "x", []byte("some"))
	time.Sleep(2*time.Second + 100*time.Millisecond)
	assertNotExists(c, t, "x")
}
