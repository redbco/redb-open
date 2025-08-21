package databases

import (
	"bufio"
	"fmt"
	"strings"
)

type argKey string

func (a argKey) String() string { return string(a) }

const (
	instanceKey        argKey = "instance"
	descriptionKey     argKey = "description"
	dbNameKey          argKey = "db-name"
	usernameKey        argKey = "username"
	passwordKey        argKey = "password"
	nodeIdKey          argKey = "node-id"
	enabledKey         argKey = "enabled"
	environmentIdKey   argKey = "environment-id"
	dbTypeKey          argKey = "type"
	dbVendorKey        argKey = "vendor"
	hostKey            argKey = "host"
	portKey            argKey = "port"
	sslEnabledKey      argKey = "ssl"
	sslModeKey         argKey = "ssl-mode"
	sslCertPathKey     argKey = "ssl-cert"
	sslKeyPathKey      argKey = "ssl-key"
	sslRootCertPathKey argKey = "ssl-root-cert"
)

func scanArgs(args []string) map[argKey]string {
	keys := []argKey{
		instanceKey,
		descriptionKey,
		dbNameKey,
		usernameKey,
		passwordKey,
		nodeIdKey,
		enabledKey,
		environmentIdKey,
		dbTypeKey,
		dbVendorKey,
		hostKey,
		portKey,
		sslEnabledKey,
		sslModeKey,
		sslCertPathKey,
		sslKeyPathKey,
		sslRootCertPathKey,
	}
	m := make(map[argKey]string, len(keys))

	for i := range keys {
		if v, ok := parseArg(args, keys[i]); ok {
			m[keys[i]] = v
		}
	}

	return m
}

func getArgOrPrompt(reader *bufio.Reader, m map[argKey]string, key argKey, question string, promptIfMissing bool) string {
	if v, ok := m[key]; ok {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}

	if !promptIfMissing {
		return ""
	}

	return prompt(reader, question)
}

func parseArg(args []string, key argKey) (string, bool) {
	for i := len(args) - 1; i >= 0; i-- {
		if strings.HasPrefix(args[i], "--"+key.String()+"=") {
			_, val, _ := strings.Cut(args[i], "=")
			return strings.TrimSpace(val), true
		}
	}
	return "", false
}

func prompt(r *bufio.Reader, q string) string {
	fmt.Print(q)
	res, _ := r.ReadString('\n')
	res = strings.TrimSpace(res)
	return res
}
