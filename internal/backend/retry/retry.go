/*
Copyright AppsCode Inc. and Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package retry

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

const (
	maxRetries = 5
	delay      = 10 * time.Second
)

var retryablePatterns = []string{
	"for anonymous authentication",
}

type RetryConfig struct {
	MaxRetries  int
	Delay       time.Duration
	ShouldRetry func(error) bool
}

func NewRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries: maxRetries,
		Delay:      delay,
		ShouldRetry: func(err error) bool {
			if err == nil {
				return false
			}
			lower_str := strings.ToLower(err.Error())
			fmt.Printf("Verifying error for retry: %s\n", lower_str)
			for _, pattern := range retryablePatterns {
				if strings.Contains(lower_str, strings.ToLower(pattern)) {
					return true
				}
			}
			return false
		},
	}
}

func (rc *RetryConfig) RunWithRetry(ctx context.Context, execFunc func() (any, error)) (any, error) {
	attempts := 0
	var lastErr error
	var output any

	err := wait.PollUntilContextCancel(
		ctx,
		rc.Delay,
		true, // Run immediately on first call
		func(ctx context.Context) (bool, error) {
			// Stop if max retries reached
			if attempts >= rc.MaxRetries {
				return false, fmt.Errorf("max retries reached")
			}
			output, lastErr = execFunc()
			if !rc.ShouldRetry(lastErr) {
				return true, nil
			}
			klog.Infoln("Retrying command after error",
				"attempt", attempts,
				"maxRetries", rc.MaxRetries,
				"error", fmt.Sprintf("%s", lastErr.Error()))
			attempts++
			return false, nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed after %d attempts: %w", attempts, lastErr)
	}

	return output, lastErr
}
