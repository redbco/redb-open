module github.com/redbco/redb-open/cmd/cli

go 1.23.0

toolchain go1.23.5

require (
	github.com/redbco/redb-open/pkg v0.0.0
	github.com/spf13/cobra v1.9.1
	golang.org/x/term v0.33.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	al.essio.dev/pkg/shellescape v1.5.1 // indirect
	github.com/chzyer/readline v1.5.1 // indirect
	github.com/danieljoos/wincred v1.2.2 // indirect
	github.com/godbus/dbus/v5 v5.1.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/zalando/go-keyring v0.2.6 // indirect
	golang.org/x/sys v0.34.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

replace github.com/redbco/redb-open/pkg => ../../pkg
