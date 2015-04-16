package main

import (
	"cuckood"
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
	c := cuckoo.New()
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
	c := cuckoo.New()
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
	c := cuckoo.New()
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
	c := cuckoo.New()
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
	c := cuckoo.New()
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
	c := cuckoo.New()
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
	c := cuckoo.New()
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
	c := cuckoo.New()
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
	c := cuckoo.New()
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
	c := cuckoo.New()
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
	c := cuckoo.New()
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
	c := cuckoo.New()
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
	c := cuckoo.New()
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
	c := cuckoo.New()
	assertPM(c, t, "x", 1, 5, false, gomem.DECREMENT)
	assertGet(c, t, "x", []byte("5"))
	assertPM(c, t, "x", 1, 0, false, gomem.DECREMENT)
	assertGet(c, t, "x", []byte("4"))
	assertPM(c, t, "x", 211, 0, false, gomem.DECREMENT)
	assertGet(c, t, "x", []byte("0"))
}
