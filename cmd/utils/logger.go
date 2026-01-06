package utils

import (
	"fmt"

	"go.uber.org/zap"
)

// NewSugaredLogger creates a sugared logger based on the verbose flag.
// If verbose is true, it creates a development logger, otherwise a production logger.
func NewSugaredLogger(verbose bool) (*zap.SugaredLogger, error) {
	if verbose {
		l, err := zap.NewDevelopment()
		if err != nil {
			return nil, fmt.Errorf("failed to create development logger: %w", err)
		}
		return l.Sugar(), nil
	}

	l, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to create production logger: %w", err)
	}
	return l.Sugar(), nil
}
