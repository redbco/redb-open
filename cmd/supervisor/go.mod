module github.com/redbco/redb-open/cmd/supervisor

go 1.24.0

toolchain go1.24.9

require (
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.7.5
	github.com/redbco/redb-open/api v0.0.0
	github.com/redbco/redb-open/pkg v0.0.0
	golang.org/x/crypto v0.42.0
	golang.org/x/term v0.35.0
	google.golang.org/grpc v1.75.1
	google.golang.org/protobuf v1.36.9
	gopkg.in/yaml.v3 v3.0.1
)

require (
	al.essio.dev/pkg/shellescape v1.5.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/danieljoos/wincred v1.2.2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/godbus/dbus/v5 v5.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/redis/go-redis/v9 v9.11.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/zalando/go-keyring v0.2.6 // indirect
	golang.org/x/net v0.44.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/text v0.30.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250908214217-97024824d090 // indirect
)

replace github.com/redbco/redb-open/pkg => ../../pkg

replace github.com/redbco/redb-open/api => ../../api
