package ports

import (
	"context"
	"graph-alert-engine/internal/core/domain"
)

type GraphProvider interface {
	GetServices(ctx context.Context) ([]domain.ServiceNode, error)
	GetCentrality(ctx context.Context) (map[string]domain.Centrality, error) // key = ServiceNode.ID()
	GetPeers(ctx context.Context, svc domain.ServiceNode, direction domain.Direction) ([]domain.ServiceNode, error)
}
