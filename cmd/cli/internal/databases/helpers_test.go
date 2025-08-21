package databases

import (
	"bufio"
	"strings"
	"testing"
)

func rdr(s string) *bufio.Reader { return bufio.NewReader(strings.NewReader(s)) }

func TestInstanceParam(t *testing.T) {
	t.Run("from args", func(t *testing.T) {
		m := map[argKey]string{instanceKey: "inst-42"}
		got := instanceParam(rdr("ignored\n"), m)
		if got != "inst-42" {
			t.Fatalf("want inst-42, got %q", got)
		}
	})
	t.Run("from prompt", func(t *testing.T) {
		m := map[argKey]string{}
		got := instanceParam(rdr("inst-from-prompt\n"), m)
		if got != "inst-from-prompt" {
			t.Fatalf("want inst-from-prompt, got %q", got)
		}
	})
}

func TestDescriptionParam(t *testing.T) {
	t.Run("from args", func(t *testing.T) {
		m := map[argKey]string{descriptionKey: "desc"}
		if got := descriptionParam(rdr("x\n"), m); got != "desc" {
			t.Fatalf("want desc, got %q", got)
		}
	})
	t.Run("from prompt", func(t *testing.T) {
		m := map[argKey]string{}
		if got := descriptionParam(rdr("my desc\n"), m); got != "my desc" {
			t.Fatalf("want my desc, got %q", got)
		}
	})
}

func TestDBNameParam(t *testing.T) {
	t.Run("ok from args", func(t *testing.T) {
		m := map[argKey]string{dbNameKey: "db1"}
		got, err := dbNameParam(rdr("x\n"), m)
		if err != nil || got != "db1" {
			t.Fatalf("want db1,nil got %q,%v", got, err)
		}
	})
	t.Run("ok from prompt", func(t *testing.T) {
		m := map[argKey]string{}
		got, err := dbNameParam(rdr("db2\n"), m)
		if err != nil || got != "db2" {
			t.Fatalf("want db2,nil got %q,%v", got, err)
		}
	})
	t.Run("empty -> error", func(t *testing.T) {
		m := map[argKey]string{}
		_, err := dbNameParam(rdr("\n"), m)
		if err == nil {
			t.Fatal("expected error on empty DB Name")
		}
	})
}

func TestUsernameAndPassword(t *testing.T) {
	t.Run("both from args", func(t *testing.T) {
		m := map[argKey]string{usernameKey: "u", passwordKey: "p"}
		u, p, err := usernameAndPassword(rdr("x\n"), m)
		if err != nil || u != "u" || p != "p" {
			t.Fatalf("want u,p,nil got %q,%q,%v", u, p, err)
		}
	})
	t.Run("username empty, password from args", func(t *testing.T) {
		m := map[argKey]string{passwordKey: "p"}
		u, p, err := usernameAndPassword(rdr("\n"), m)
		if err != nil || u != "" || p != "p" {
			t.Fatalf("want \"\",p,nil got %q,%q,%v", u, p, err)
		}
	})
	t.Run("both empty -> no prompt for password", func(t *testing.T) {
		m := map[argKey]string{}
		u, p, err := usernameAndPassword(rdr("\n"), m)
		if err != nil || u != "" || p != "" {
			t.Fatalf("want empty user/pass,nil got %q,%q,%v", u, p, err)
		}
	})
}

func TestEnabledParam(t *testing.T) {
	t.Run("true from args", func(t *testing.T) {
		m := map[argKey]string{enabledKey: "true"}
		got, err := enabledParam(rdr("x\n"), m)
		if err != nil || !got {
			t.Fatalf("want true,nil got %v,%v", got, err)
		}
	})
	t.Run("false from prompt", func(t *testing.T) {
		m := map[argKey]string{}
		got, err := enabledParam(rdr("false\n"), m)
		if err != nil || got {
			t.Fatalf("want false,nil got %v,%v", got, err)
		}
	})
	t.Run("invalid -> error", func(t *testing.T) {
		m := map[argKey]string{enabledKey: "maybe"}
		_, err := enabledParam(rdr("x\n"), m)
		if err == nil {
			t.Fatal("expected error on invalid enabled")
		}
	})
	t.Run("empty -> error", func(t *testing.T) {
		m := map[argKey]string{}
		_, err := enabledParam(rdr("\n"), m)
		if err == nil {
			t.Fatal("expected error on empty enabled")
		}
	})
}

func TestDBTypeParam(t *testing.T) {
	t.Run("from args", func(t *testing.T) {
		m := map[argKey]string{dbTypeKey: "postgres"}
		got, err := dbTypeParam(rdr("x\n"), m)
		if err != nil || got != "postgres" {
			t.Fatalf("want postgres,nil got %q,%v", got, err)
		}
	})
	t.Run("from prompt", func(t *testing.T) {
		m := map[argKey]string{}
		got, err := dbTypeParam(rdr("mysql\n"), m)
		if err != nil || got != "mysql" {
			t.Fatalf("want mysql,nil got %q,%v", got, err)
		}
	})
	t.Run("empty -> error", func(t *testing.T) {
		m := map[argKey]string{}
		_, err := dbTypeParam(rdr("\n"), m)
		if err == nil {
			t.Fatal("expected error on empty db type")
		}
	})
}

func TestDBVendorParam(t *testing.T) {
	t.Run("from args", func(t *testing.T) {
		m := map[argKey]string{dbVendorKey: "aws"}
		if got := dbVendorParam(rdr("x\n"), m); got != "aws" {
			t.Fatalf("want aws, got %q", got)
		}
	})
	t.Run("default custom", func(t *testing.T) {
		m := map[argKey]string{}
		if got := dbVendorParam(rdr("x\n"), m); got != "custom" {
			t.Fatalf("want custom, got %q", got)
		}
	})
}

func TestHostParam(t *testing.T) {
	t.Run("from args", func(t *testing.T) {
		m := map[argKey]string{hostKey: "db.local"}
		got, err := hostParam(rdr("x\n"), m)
		if err != nil || got != "db.local" {
			t.Fatalf("want db.local,nil got %q,%v", got, err)
		}
	})
	t.Run("from prompt", func(t *testing.T) {
		m := map[argKey]string{}
		got, err := hostParam(rdr("example.com\n"), m)
		if err != nil || got != "example.com" {
			t.Fatalf("want example.com,nil got %q,%v", got, err)
		}
	})
	t.Run("empty -> error", func(t *testing.T) {
		m := map[argKey]string{}
		_, err := hostParam(rdr("\n"), m)
		if err == nil {
			t.Fatal("expected error on empty host")
		}
	})
}

func TestPortParam(t *testing.T) {
	t.Run("from args valid", func(t *testing.T) {
		m := map[argKey]string{portKey: "5432"}
		got, err := portParam(rdr("x\n"), m)
		if err != nil || got != 5432 {
			t.Fatalf("want 5432,nil got %d,%v", got, err)
		}
	})
	t.Run("from prompt valid", func(t *testing.T) {
		m := map[argKey]string{}
		got, err := portParam(rdr("3306\n"), m)
		if err != nil || got != 3306 {
			t.Fatalf("want 3306,nil got %d,%v", got, err)
		}
	})
	t.Run("non-int -> error", func(t *testing.T) {
		m := map[argKey]string{portKey: "abc"}
		_, err := portParam(rdr("x\n"), m)
		if err == nil {
			t.Fatal("expected error on non-int port")
		}
	})
	t.Run("out of range -> error", func(t *testing.T) {
		m := map[argKey]string{portKey: "70000"}
		_, err := portParam(rdr("x\n"), m)
		if err == nil {
			t.Fatal("expected error on out-of-range port")
		}
	})
	t.Run("empty -> error", func(t *testing.T) {
		m := map[argKey]string{}
		_, err := portParam(rdr("\n"), m)
		if err == nil {
			t.Fatal("expected error on empty port")
		}
	})
}

func TestSSLParam(t *testing.T) {
	t.Run("true from args", func(t *testing.T) {
		m := map[argKey]string{sslEnabledKey: "true"}
		got, err := sslParam(rdr("x\n"), m)
		if err != nil || !got {
			t.Fatalf("want true,nil got %v,%v", got, err)
		}
	})
	t.Run("false from prompt", func(t *testing.T) {
		m := map[argKey]string{}
		got, err := sslParam(rdr("false\n"), m)
		if err != nil || got {
			t.Fatalf("want false,nil got %v,%v", got, err)
		}
	})
	t.Run("invalid -> error", func(t *testing.T) {
		m := map[argKey]string{sslEnabledKey: "maybe"}
		_, err := sslParam(rdr("x\n"), m)
		if err == nil {
			t.Fatal("expected error on invalid ssl")
		}
	})
	t.Run("empty -> error", func(t *testing.T) {
		m := map[argKey]string{}
		_, err := sslParam(rdr("\n"), m)
		if err == nil {
			t.Fatal("expected error on empty ssl")
		}
	})
}

func TestSSLModeParam(t *testing.T) {
	t.Run("valid from args", func(t *testing.T) {
		m := map[argKey]string{sslModeKey: "require"}
		got, err := sslModeParam(rdr("x\n"), m)
		if err != nil || got != "require" {
			t.Fatalf("want require,nil got %q,%v", got, err)
		}
	})
	t.Run("valid from prompt", func(t *testing.T) {
		m := map[argKey]string{}
		got, err := sslModeParam(rdr("prefer\n"), m)
		if err != nil || got != "prefer" {
			t.Fatalf("want prefer,nil got %q,%v", got, err)
		}
	})
	t.Run("invalid -> error", func(t *testing.T) {
		m := map[argKey]string{sslModeKey: "weird"}
		_, err := sslModeParam(rdr("x\n"), m)
		if err == nil {
			t.Fatal("expected error on invalid mode")
		}
	})
	t.Run("empty -> error", func(t *testing.T) {
		m := map[argKey]string{}
		_, err := sslModeParam(rdr("\n"), m)
		if err == nil {
			t.Fatal("expected error on empty mode")
		}
	})
}

func TestSSLSetup(t *testing.T) {
	t.Run("ssl=false => disable", func(t *testing.T) {
		m := map[argKey]string{sslEnabledKey: "false"}
		ssl, mode, err := sslSetup(rdr("x\n"), m)
		if err != nil || ssl || mode != "disable" {
			t.Fatalf("want false,disable,nil got %v,%q,%v", ssl, mode, err)
		}
	})
	t.Run("ssl=true, mode from args", func(t *testing.T) {
		m := map[argKey]string{sslEnabledKey: "true", sslModeKey: "require"}
		ssl, mode, err := sslSetup(rdr("x\n"), m)
		if err != nil || !ssl || mode != "require" {
			t.Fatalf("want true,require,nil got %v,%q,%v", ssl, mode, err)
		}
	})
	t.Run("ssl=true, invalid mode -> error", func(t *testing.T) {
		m := map[argKey]string{sslEnabledKey: "true", sslModeKey: "bad"}
		_, _, err := sslSetup(rdr("x\n"), m)
		if err == nil {
			t.Fatal("expected error on bad ssl mode")
		}
	})
}
