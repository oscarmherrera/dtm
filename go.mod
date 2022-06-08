module github.com/dtm-labs/dtm

go 1.17

require (
	bou.ke/monkey v1.0.2
	github.com/BurntSushi/toml v0.4.1 // indirect
	github.com/aerospike/aerospike-client-go/v5 v5.8.0
	github.com/dtm-labs/dtmdriver v0.0.3
	github.com/dtm-labs/dtmdriver-gozero v0.0.5
	github.com/dtm-labs/dtmdriver-http v1.2.2
	github.com/dtm-labs/dtmdriver-kratos v0.0.8
	github.com/dtm-labs/dtmdriver-polaris v0.0.4
	github.com/dtm-labs/dtmdriver-protocol1 v0.0.1
	github.com/gin-gonic/gin v1.7.7
	github.com/go-playground/validator/v10 v10.11.0 // indirect
	github.com/go-redis/redis/v8 v8.11.4
	github.com/go-resty/resty/v2 v2.7.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/lib/pq v1.10.4
	github.com/lithammer/shortuuid v2.0.3+incompatible
	github.com/lithammer/shortuuid/v3 v3.0.7
	github.com/natefinch/lumberjack v2.0.0+incompatible
	github.com/onsi/gomega v1.16.0
	github.com/prometheus/client_golang v1.11.0
	github.com/rs/xid v1.2.1
	github.com/silenceper/pool v1.0.0
	github.com/stretchr/testify v1.7.1
	github.com/ugorji/go v1.2.7 // indirect
	go.etcd.io/bbolt v1.3.6
	go.mongodb.org/mongo-driver v1.9.0
	go.uber.org/automaxprocs v1.5.1
	go.uber.org/zap v1.21.0
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
	google.golang.org/grpc v1.46.2
	google.golang.org/protobuf v1.28.0
	gopkg.in/yaml.v3 v3.0.1
	gorm.io/driver/mysql v1.0.3
	gorm.io/driver/postgres v1.2.1
	gorm.io/gorm v1.22.2
// gotest.tools v2.2.0+incompatible
)

replace google.golang.org/grpc v1.46.2 => github.com/grpc/grpc-go v1.47.0
// replace github.com/dtm-labs/dtmdriver v0.0.2 => /Users/wangxi/dtm/dtmdriver

// replace github.com/dtm-labs/dtmdriver-http => /Users/wangxi/dtm/dtmdriver-http-nacos