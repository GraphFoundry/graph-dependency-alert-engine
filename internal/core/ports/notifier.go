package ports

import (
	"context"
	"graph-alert-engine/internal/core/domain"
)

type Notifier interface {
	Notify(ctx context.Context, alert domain.Alert) error
}
