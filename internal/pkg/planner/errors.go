package planner

import "github.com/pkg/errors"

var (
	ErrFailedToCreatePlan = errors.New("failed to create plan")
	ErrExecuteError       = errors.New("execute error")
)
