module github.com/redbco/redb-open/cmd/cli

go 1.24.0

toolchain go1.24.9

require (
	github.com/chzyer/readline v1.5.1
	github.com/redbco/redb-open/pkg v0.0.0
	github.com/spf13/cobra v1.9.1
	github.com/spf13/pflag v1.0.6
	golang.org/x/term v0.35.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	al.essio.dev/pkg/shellescape v1.5.1 // indirect
	github.com/danieljoos/wincred v1.2.2 // indirect
	github.com/godbus/dbus/v5 v5.1.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/zalando/go-keyring v0.2.6 // indirect
	golang.org/x/sys v0.36.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

replace github.com/redbco/redb-open/pkg => ../../pkg
