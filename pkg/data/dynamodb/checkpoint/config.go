package checkpoint

import "go.uber.org/zap"

type Config struct {
	Region      string
	TableName   string
	EndpointURL string
	Logger      *zap.SugaredLogger
}
