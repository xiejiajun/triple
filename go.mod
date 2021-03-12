module github.com/dubbogo/triple

go 1.15

//replace github.com/apache/dubbo-go v1.5.5 => ../dubbo-go

require (
	github.com/apache/dubbo-go v1.5.5
	github.com/dubbogo/net v0.0.2-0.20210312095142-e0595532fde0
	github.com/golang/protobuf v1.4.3
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	go.uber.org/atomic v1.7.0
	golang.org/x/net v0.0.0-20201224014010-6772e930b67b
	golang.org/x/sys v0.0.0-20201223074533-0d417f636930
	google.golang.org/genproto v0.0.0-20210106152847-07624b53cd92
	google.golang.org/grpc v1.33.1
	gotest.tools v2.2.0+incompatible
)
