package s3

import (
	"strings"
	"testing"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

var testcases = map[string]string{
	"/a":     "a",
	"/a/b":   "ab",
	"/a/b/c": "abc",
	"/a/b/d": "a/b/d",
	"/a/c":   "ac",
	"/a/d":   "ad",
	"/e":     "e",
	"/f":     "f",
}

// set this to the name of the bucket you'd like to test with
const bucketName = "YOUR_TEST_BUCKET_NAME"

func newDS(t *testing.T) *Datastore {
	return NewDatastore(bucketName, func(o *Options) {
		o.Region = "us-east-1"
	})
}

func addTestCases(t *testing.T, d *Datastore, testcases map[string]string) {
	for k, v := range testcases {
		dsk := ds.NewKey(k)
		if err := d.Put(dsk, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range testcases {
		dsk := ds.NewKey(k)
		v2, err := d.Get(dsk)
		if err != nil {
			t.Fatal(err)
		}
		if v2b, ok := v2.([]byte); ok {
			if string(v2b) != v {
				t.Errorf("%s values differ: '%s' != '%s'", k, v, v2b)
			}
		} else {
			t.Errorf("%s value isn't []byte: %s", k, v2)
		}
	}

}

func TestGet(t *testing.T) {
	d := newDS(t)
	expectErrors(func(key ds.Key) error {
		_, err := d.Get(key)
		return err
	}, t)
}

func TestHas(t *testing.T) {
	d := newDS(t)
	if has, err := d.Has(ds.NewKey("/z")); has != false || err != nil {
		t.Errorf("has on empty key result mismatch: %t != false || %s != nil", has, err)
	}
}

func TestDelete(t *testing.T) {
	d := newDS(t)
	expectErrors(d.Delete, t)
}

func TestQuery(t *testing.T) {
	d := newDS(t)
	addTestCases(t, d, testcases)

	rs, err := d.Query(dsq.Query{Prefix: "/a/"})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b",
		"/a/b/c",
		"/a/b/d",
		"/a/c",
		"/a/d",
	}, rs)

	// test offset and limit

	rs, err = d.Query(dsq.Query{Prefix: "/a/", Offset: 2, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		// TODO - this returns a different result due to the behaviour of s3's list objects
		// "/a/b/d",
		"/a/d",
		"/a/c",
	}, rs)

}

func TestQueryRespectsProcess(t *testing.T) {
	d := newDS(t)
	addTestCases(t, d, testcases)
}

func expectMatches(t *testing.T, expect []string, actualR dsq.Results) {
	actual, err := actualR.Rest()
	if err != nil {
		t.Error(err)
	}

	if len(actual) != len(expect) {
		t.Error("length mismatch", expect, actual)
	}

	for _, k := range expect {
		found := false
		for _, e := range actual {
			if e.Key == k {
				found = true
			}
		}
		if !found {
			t.Error(k, "not found")
		}
	}
}

func expectErrors(fn func(key ds.Key) error, t *testing.T) {
	errCases := []struct {
		key   string
		err   error
		exact bool
	}{
		{"/z", ds.ErrNotFound, true},
	}

	for i, c := range errCases {
		got := fn(ds.NewKey(c.key))
		if got == nil {
			t.Errorf("no error returned for %d mismatch. %s != %s", i, got, c.err)
			continue
		}

		if c.exact {
			if got != c.err {
				t.Errorf("error case %d mismatch. %s != %s", i, got, c.err)
			}
		} else if !strings.Contains(got.Error(), c.err.Error()) {
			t.Errorf("error case %d mismatch. %s != %s", i, got, c.err)
		}
	}
}

// TODO
// func TestBatching(t *testing.T) {
// 	d := newDS(t)

// 	b, err := d.Batch()
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	for k, v := range testcases {
// 		err := b.Put(ds.NewKey(k), []byte(v))
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 	}

// 	err = b.Commit()
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	for k, v := range testcases {
// 		val, err := d.Get(ds.NewKey(k))
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		if v != string(val.([]byte)) {
// 			t.Fatal("got wrong data!")
// 		}
// 	}
// }
