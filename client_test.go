package ftp

import (
	"bytes"
	"io/ioutil"
	"net/textproto"
	"strings"
	"testing"
	"time"
)

const (
	testData = "Just some text"
	testDir  = "mydir"
)

func TestConnPASV(t *testing.T) {
	testConn(t, true)
}

func TestConnEPSV(t *testing.T) {
	testConn(t, false)
}

func testConn(t *testing.T, disableEPSV bool) {

	mock, c := openConn(t, "127.0.0.1", DialWithTimeout(5*time.Second), DialWithDisabledEPSV(disableEPSV))

	err := c.Login(ctx, "anonymous", "anonymous")
	if err != nil {
		t.Fatal(err)
	}

	err = c.NoOp(ctx)
	if err != nil {
		t.Error(err)
	}

	err = c.ChangeDir(ctx, "incoming")
	if err != nil {
		t.Error(err)
	}

	dir, err := c.CurrentDir(ctx)
	if err != nil {
		t.Error(err)
	} else {
		if dir != "/incoming" {
			t.Error("Wrong dir: " + dir)
		}
	}

	data := bytes.NewBufferString(testData)
	_, err = c.Stor(ctx, "test", data)
	if err != nil {
		t.Error(err)
	}

	_, err = c.List(ctx, ".")
	if err != nil {
		t.Error(err)
	}

	_, err = c.Rename(ctx, "test", "tset")
	if err != nil {
		t.Error(err)
	}

	// Read without deadline
	r, err := c.Retr(ctx, "tset")
	if err != nil {
		t.Error(err)
	} else {
		buf, err := ioutil.ReadAll(r)
		if err != nil {
			t.Error(err)
		}
		if string(buf) != testData {
			t.Errorf("'%s'", buf)
		}
		r.Close()
		r.Close() // test we can close two times
	}

	// Read with deadline
	r, err = c.Retr(ctx, "tset")
	if err != nil {
		t.Error(err)
	} else {
		r.SetDeadline(time.Now())
		_, err := ioutil.ReadAll(r)
		if err == nil {
			t.Error("deadline should have caused error")
		} else if !strings.HasSuffix(err.Error(), "i/o timeout") {
			t.Error(err)
		}
		r.Close()
	}

	// Read with offset
	r, err = c.RetrFrom(ctx, "tset", 5)
	if err != nil {
		t.Error(err)
	} else {
		buf, err := ioutil.ReadAll(r)
		if err != nil {
			t.Error(err)
		}
		expected := testData[5:]
		if string(buf) != expected {
			t.Errorf("read %q, expected %q", buf, expected)
		}
		r.Close()
	}

	fileSize, err := c.FileSize(ctx, "magic-file")
	if err != nil {
		t.Error(err)
	}
	if fileSize != 42 {
		t.Errorf("file size %q, expected %q", fileSize, 42)
	}

	_, err = c.FileSize(ctx, "not-found")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	_, err = c.Delete(ctx, "tset")
	if err != nil {
		t.Error(err)
	}

	_, err = c.MakeDir(ctx, testDir)
	if err != nil {
		t.Error(err)
	}

	err = c.ChangeDir(ctx, testDir)
	if err != nil {
		t.Error(err)
	}

	err = c.ChangeDirToParent(ctx)
	if err != nil {
		t.Error(err)
	}

	entries, err := c.NameList(ctx, "/")
	if err != nil {
		t.Error(err)
	}
	if len(entries) != 1 || entries[0] != "/incoming" {
		t.Errorf("Unexpected entries: %v", entries)
	}

	_, err = c.RemoveDir(ctx, testDir)
	if err != nil {
		t.Error(err)
	}

	err = c.Logout(ctx)
	if err != nil {
		if protoErr := err.(*textproto.Error); protoErr != nil {
			if protoErr.Code != StatusNotImplemented {
				t.Error(err)
			}
		} else {
			t.Error(err)
		}
	}

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}

	// Wait for the connection to close
	mock.Wait()

	err = c.NoOp(ctx)
	if err == nil {
		t.Error("Expected error")
	}
}

func TestTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	c, err := DialTimeout(ctx, "localhost:2121", 1*time.Second)
	if err == nil {
		t.Fatal("expected timeout, got nil error")
		c.Quit()
	}
}

func TestWrongLogin(t *testing.T) {
	mock, err := newFtpMock(t, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	c, err := DialTimeout(ctx, mock.Addr(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Quit()

	err = c.Login(ctx, "zoo2Shia", "fei5Yix9")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestDeleteDirRecur(t *testing.T) {
	mock, c := openConn(t, "127.0.0.1")

	_, err := c.RemoveDirRecur(ctx, "testDir")
	if err != nil {
		t.Error(err)
	}

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}

	// Wait for the connection to close
	mock.Wait()
}

// func TestFileDeleteDirRecur(t *testing.T) {
// 	mock, c := openConn(t, "127.0.0.1")

// 	err := c.RemoveDirRecur("testFile")
// 	if err == nil {
// 		t.Fatal("expected error got nil")
// 	}

// 	if err := c.Quit(); err != nil {
// 		t.Fatal(err)
// 	}

// 	// Wait for the connection to close
// 	mock.Wait()
// }

func TestMissingFolderDeleteDirRecur(t *testing.T) {
	mock, c := openConn(t, "127.0.0.1")

	_, err := c.RemoveDirRecur(ctx, "missing-dir")
	if err == nil {
		t.Fatal("expected error got nil")
	}

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}

	// Wait for the connection to close
	mock.Wait()
}
